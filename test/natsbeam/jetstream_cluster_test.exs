defmodule Natsbeam.JetStreamClusterTest do
  @moduledoc "Replicated streams across BEAM nodes: placement, quorum acks, failover, stepdown."
  use ExUnit.Case, async: false
  import Natsbeam.TestHelpers
  import Natsbeam.ClusterHelpers
  alias Natsbeam.JetStream.Stream, as: JStream
  @moduletag :cluster
  @moduletag timeout: 120_000

  @acc "$G"

  setup_all do
    ensure_distributed!()
    {p1, n1} = start_peer!(:js_peer1)
    {p2, n2} = start_peer!(:js_peer2)

    for n <- [node(), n1, n2] do
      assert wait_until(
               fn -> length(call(n, Natsbeam.JetStream.Cluster, :nodes, [])) == 3 end,
               10_000
             )
    end

    on_exit(fn ->
      stop_peer(p1)
      stop_peer(p2)
    end)

    {:ok, peers: [{p1, n1}, {p2, n2}]}
  end

  setup do
    reset_config!()
    for info <- Natsbeam.streams(@acc), do: api("STREAM.DELETE.#{info["config"]["name"]}", "")
    :ok
  end

  defp api(op, body, node \\ node())
  defp api(op, body, node) when is_map(body), do: api(op, Jason.encode!(body), node)

  defp api(op, body, node) do
    {:ok, %{payload: p}} =
      call(node, Natsbeam.Local, :request, [
        "$JS.API." <> op,
        body,
        [account: @acc, timeout: 5_000]
      ])

    Jason.decode!(p)
  end

  defp pub(subject, data, node \\ node()) do
    {:ok, %{payload: p}} =
      call(node, Natsbeam.Local, :request, [subject, data, [account: @acc, timeout: 5_000]])

    Jason.decode!(p)
  end

  defp replica_state(node, name) do
    case call(node, JStream, :local_replica, [@acc, name]) do
      pid when is_pid(pid) -> call(node, GenServer, :call, [pid, :repl_state])
      _ -> nil
    end
  end

  defp leader_node(name) do
    {:ok, pid} = JStream.lookup(@acc, name)
    node(pid)
  end

  test "R3 stream is replicated with quorum acks and visible from every node", %{peers: peers} do
    [{_, n1}, {_, n2}] = peers
    cfg = %{"name" => "R3", "subjects" => ["r3.>"], "storage" => "file", "num_replicas" => 3}
    resp = api("STREAM.CREATE.R3", cfg)
    refute Map.has_key?(resp, "error"), inspect(resp)
    assert %{"cluster" => %{"leader" => leader, "replicas" => replicas}} = resp
    assert leader == Natsbeam.Server.info().name
    assert length(replicas) == 2

    for i <- 1..5, do: assert(%{"seq" => ^i} = pub("r3.a", "m#{i}"))

    # replicas caught up, info from a peer agrees, listing works everywhere
    assert wait_until(fn -> match?(%{last_seq: 5, msgs: 5}, replica_state(n1, "R3")) end)
    assert wait_until(fn -> match?(%{last_seq: 5, msgs: 5}, replica_state(n2, "R3")) end)

    assert %{"state" => %{"messages" => 5}, "cluster" => %{"replicas" => [_, _]}} =
             api("STREAM.INFO.R3", "", n1)

    assert %{"streams" => ["R3"]} = api("STREAM.NAMES", "", n2)

    assert %{"error" => %{"err_code" => 10005}} =
             api("STREAM.CREATE.R9", %{"name" => "R9", "subjects" => ["r9"], "num_replicas" => 4})

    # a consumer used from another node: pull, ack
    assert %{"name" => "C"} =
             api(
               "CONSUMER.CREATE.R3.C",
               %{"stream_name" => "R3", "config" => %{"durable_name" => "C"}},
               n2
             )

    {:ok, %{payload: "m1", subject: "r3.a"}} =
      call(n2, Natsbeam.Local, :request, [
        "$JS.API.CONSUMER.MSG.NEXT.R3.C",
        "{\"batch\":1}",
        [account: @acc, timeout: 5_000]
      ])

    info = api("CONSUMER.INFO.R3.C", "", n1)
    assert info["delivered"]["stream_seq"] == 1 and info["num_ack_pending"] == 1

    # deletes replicate, and stream deletion removes the replicas
    api("STREAM.MSG.DELETE.R3", %{"seq" => 2})
    assert wait_until(fn -> match?(%{msgs: 4}, replica_state(n1, "R3")) end)
    assert %{"success" => true} = api("STREAM.DELETE.R3", "")
    assert wait_until(fn -> replica_state(n1, "R3") == nil and replica_state(n2, "R3") == nil end)
  end

  test "stepdown moves leadership and keeps data and consumers", %{peers: peers} do
    [{_, n1}, _] = peers

    api("STREAM.CREATE.STEP", %{
      "name" => "STEP",
      "subjects" => ["step.>"],
      "storage" => "memory",
      "num_replicas" => 3
    })

    for i <- 1..3, do: pub("step.a", "m#{i}")

    api("CONSUMER.CREATE.STEP.D", %{"stream_name" => "STEP", "config" => %{"durable_name" => "D"}})

    assert wait_until(fn -> match?(%{last_seq: 3}, replica_state(n1, "STEP")) end)
    old = leader_node("STEP")

    assert %{"success" => true} = api("STREAM.LEADER.STEPDOWN.STEP", "")

    assert wait_until(
             fn ->
               (lu = JStream.lookup(@acc, "STEP")) != {:error, :stream_not_found} and
                 node(elem(lu, 1)) != old
             end,
             10_000
           )

    new = leader_node("STEP")
    assert new != old
    assert %{"state" => %{"messages" => 3, "last_seq" => 3}} = api("STREAM.INFO.STEP", "")
    assert wait_until(fn -> match?(%{"name" => "D"}, api("CONSUMER.INFO.STEP.D", "")) end, 10_000)
    assert %{"seq" => 4} = pub("step.a", "m4")

    {:ok, %{payload: "m1"}} =
      Natsbeam.request("$JS.API.CONSUMER.MSG.NEXT.STEP.D", "{\"batch\":1}",
        account: @acc,
        timeout: 5_000
      )
  end

  test "losing the leader node elects a replica and resumes", %{peers: peers} do
    [_, {_, n2}] = peers
    # a dedicated third peer leads this stream and is then killed
    {p3, n3} = start_peer!(:js_peer3)

    for n <- [node(), n2, n3] do
      assert wait_until(
               fn -> length(call(n, Natsbeam.JetStream.Cluster, :nodes, [])) == 4 end,
               10_000
             )
    end

    resp =
      api(
        "STREAM.CREATE.FAIL",
        %{"name" => "FAIL", "subjects" => ["fail.>"], "storage" => "file", "num_replicas" => 3},
        n3
      )

    refute Map.has_key?(resp, "error"), inspect(resp)
    assert resp["cluster"]["leader"] == call(n3, Natsbeam.Server, :info, []).name
    for i <- 1..3, do: pub("fail.a", "m#{i}", n3)

    api(
      "CONSUMER.CREATE.FAIL.D",
      %{"stream_name" => "FAIL", "config" => %{"durable_name" => "D"}},
      n3
    )

    {:ok, %{payload: "m1", reply: r}} =
      Natsbeam.request("$JS.API.CONSUMER.MSG.NEXT.FAIL.D", "{\"batch\":1}",
        account: @acc,
        timeout: 5_000
      )

    Natsbeam.publish(r, "+ACK", account: @acc)

    replica_nodes =
      Enum.filter([node(), n2, hd(peers) |> elem(1)], &(replica_state(&1, "FAIL") != nil))

    assert length(replica_nodes) == 2

    for n <- replica_nodes,
        do:
          assert(wait_until(fn -> match?(%{last_seq: 3, msgs: 3}, replica_state(n, "FAIL")) end))

    # let the consumer checkpoint replicate
    Process.sleep(2_500)

    stop_peer(p3)

    assert wait_until(
             fn -> match?({:ok, pid} when node(pid) != n3, JStream.lookup(@acc, "FAIL")) end,
             15_000
           )

    assert %{"state" => %{"messages" => 3}} = api("STREAM.INFO.FAIL", "")
    assert %{"seq" => 4} = pub("fail.a", "m4")
    assert wait_until(fn -> match?(%{"name" => "D"}, api("CONSUMER.INFO.FAIL.D", "")) end, 10_000)
    info = api("CONSUMER.INFO.FAIL.D", "")
    assert info["ack_floor"]["stream_seq"] == 1

    {:ok, %{payload: "m2"}} =
      Natsbeam.request("$JS.API.CONSUMER.MSG.NEXT.FAIL.D", "{\"batch\":1}",
        account: @acc,
        timeout: 5_000
      )

    api("STREAM.DELETE.FAIL", "")
  end
end
