defmodule Natsbeam.GoLeafTest do
  @moduledoc "natsbeam joined to a real Go nats-server over the leafnode protocol."
  use ExUnit.Case, async: false
  import Natsbeam.TestHelpers
  alias Natsbeam.GoServer
  @moduletag :go
  @moduletag timeout: 120_000

  # `go_extra:` adds raw config to the Go hub.
  setup context do
    reset_config!()
    go = GoServer.start!(domain: "hub", extra: context[:go_extra] || "")
    port = start_listener!()
    {:ok, go: go, port: port}
  end

  # Both sides share the default account across the leaf link.
  defp link!(go) do
    {:ok, pid} = Natsbeam.add_leaf_remote("nats-leaf://127.0.0.1:#{go.leaf}", account: "$G")
    assert wait_until(fn -> Natsbeam.Link.status(pid).connected end, 10_000)
    pid
  end

  test "core pub/sub, request/reply and queue groups flow both ways", %{go: go, port: port} do
    link!(go)

    # subscriber on the Go side, publisher on the natsbeam side
    sub = nats_bg(go.client, ["sub", "--count", "1", "x.>"])
    collect(sub, ~r/Subscribing/)

    assert wait_until(
             fn ->
               nats(port, ["pub", "x.1", "hello"]) |> elem(1) == 0 and
                 collect(sub, ~r/hello/, 300) =~ "hello"
             end,
             10_000
           )

    # subscriber on natsbeam, publisher on Go, with headers
    {:ok, sid} = Natsbeam.subscribe("y.>")

    assert wait_until(
             fn ->
               nats(go.client, ["pub", "y.1", "from go", "-H", "X-Go:1"])

               receive do
                 {:natsbeam_msg, ^sid, "y.1", _, hdrs, "from go"} ->
                   hdrs != nil and hdrs =~ "X-Go: 1"
               after
                 300 -> false
               end
             end,
             10_000
           )

    # responder on natsbeam, requester on Go
    spawn_link(fn ->
      {:ok, s} = Natsbeam.subscribe("svc.echo")

      for _ <- 1..3 do
        receive do
          {:natsbeam_msg, ^s, _, reply, _, payload} -> Natsbeam.publish(reply, "echo:" <> payload)
        end
      end
    end)

    assert wait_until(
             fn ->
               match?(
                 {out, 0} when is_binary(out),
                 nats(go.client, ["request", "svc.echo", "ping", "--timeout", "1s"])
               )
             end,
             10_000
           )

    assert nats!(go.client, ["request", "svc.echo", "ping"]) =~ "echo:ping"

    # queue group spanning both servers gets one copy per message
    q1 = nats_bg(go.client, ["sub", "--queue", "q", "--count", "6", "q.>"])
    collect(q1, ~r/Subscribing/)
    {:ok, qsid} = Natsbeam.subscribe("q.>", queue: "q")
    Process.sleep(500)
    for i <- 1..6, do: nats!(port, ["pub", "q.x", "m#{i}"])
    Process.sleep(500)

    local =
      Stream.repeatedly(fn ->
        receive do
          {:natsbeam_msg, ^qsid, _, _, _, _} -> 1
        after
          100 -> nil
        end
      end)
      |> Enum.take_while(& &1)
      |> length()

    remote = collect(q1, ~r/never/, 500) |> String.split("Received") |> length() |> Kernel.-(1)
    assert local + remote == 6
  end

  test "Go JetStream is reachable from natsbeam clients through the hub domain", %{
    go: go,
    port: port
  } do
    link!(go)

    assert nats!(port, [
             "--js-domain",
             "hub",
             "stream",
             "add",
             "GOSTREAM",
             "--subjects",
             "gs.>",
             "--storage",
             "memory",
             "--defaults"
           ]) =~ "GOSTREAM was created"

    nats!(port, ["pub", "gs.a", "one"])

    assert wait_until(
             fn ->
               nats(port, ["--js-domain", "hub", "stream", "info", "GOSTREAM"]) |> elem(0) =~
                 "Messages: 1"
             end,
             10_000
           )

    assert nats!(go.client, ["stream", "info", "GOSTREAM"]) =~ "Messages: 1"
    # a beam-domain stream mirroring the hub stream through its external API prefix
    Natsbeam.Config.update_in([:jetstream], &Map.put(&1, :domain, "beam"))
    cfg = Path.join(System.tmp_dir!(), "mirror-#{System.unique_integer([:positive])}.json")

    File.write!(
      cfg,
      Jason.encode!(%{
        "name" => "GOMIRROR",
        "storage" => "memory",
        "mirror" => %{"name" => "GOSTREAM", "external" => %{"api" => "$JS.hub.API"}}
      })
    )

    on_exit(fn -> File.rm(cfg) end)
    assert nats!(port, ["stream", "add", "GOMIRROR", "--config", cfg]) =~ "GOMIRROR was created"

    assert wait_until(
             fn -> nats(port, ["stream", "info", "GOMIRROR"]) |> elem(0) =~ "Messages: 1" end,
             15_000
           )

    nats!(port, ["pub", "gs.b", "two"])

    assert wait_until(
             fn -> nats(port, ["stream", "info", "GOMIRROR"]) |> elem(0) =~ "Messages: 2" end,
             15_000
           )

    assert nats!(port, ["stream", "get", "GOMIRROR", "2"]) =~ "two"

    # removals on the Go origin reach the beam mirror (polled STREAM.INFO)
    nats!(go.client, ["pub", "gs.c", "three"])

    assert wait_until(
             fn -> nats(port, ["stream", "info", "GOMIRROR"]) |> elem(0) =~ "Messages: 3" end,
             15_000
           )

    nats!(go.client, ["stream", "rmm", "GOSTREAM", "2", "-f"])

    assert wait_until(
             fn -> nats(port, ["stream", "info", "GOMIRROR"]) |> elem(0) =~ "Messages: 2" end,
             15_000
           )

    assert {_, code} = nats(port, ["stream", "get", "GOMIRROR", "2"])
    assert code != 0
    assert nats!(port, ["stream", "get", "GOMIRROR", "3"]) =~ "three"

    nats!(go.client, ["stream", "purge", "GOSTREAM", "-f"])

    assert wait_until(
             fn -> nats(port, ["stream", "info", "GOMIRROR"]) |> elem(0) =~ "Messages: 0" end,
             15_000
           )

    # and the mirror keeps following afterwards
    nats!(go.client, ["pub", "gs.d", "four"])

    assert wait_until(
             fn -> nats(port, ["stream", "info", "GOMIRROR"]) |> elem(0) =~ "Messages: 1" end,
             15_000
           )

    assert nats!(port, ["stream", "get", "GOMIRROR", "4"]) =~ "four"

    assert nats!(port, ["--js-domain", "hub", "kv", "add", "GOKV"]) =~ "Bucket Name: GOKV"
    nats!(port, ["--js-domain", "hub", "kv", "put", "GOKV", "k", "v"])
    assert nats!(go.client, ["kv", "get", "GOKV", "k", "--raw"]) == "v"
  end

  test "natsbeam JetStream is reachable from Go clients through the beam domain", %{
    go: go,
    port: port
  } do
    Natsbeam.Config.update_in([:jetstream], &Map.put(&1, :domain, "beam"))
    link!(go)

    assert nats!(go.client, [
             "--js-domain",
             "beam",
             "stream",
             "add",
             "BEAMSTREAM",
             "--subjects",
             "bs.>",
             "--storage",
             "memory",
             "--defaults"
           ]) =~ "BEAMSTREAM was created"

    nats!(go.client, ["pub", "bs.a", "one"])

    assert wait_until(
             fn -> nats(port, ["stream", "info", "BEAMSTREAM"]) |> elem(0) =~ "Messages: 1" end,
             10_000
           )

    assert nats!(go.client, [
             "--js-domain",
             "beam",
             "consumer",
             "add",
             "BEAMSTREAM",
             "C",
             "--pull",
             "--ack",
             "explicit",
             "--defaults"
           ]) =~ "BEAMSTREAM > C"

    assert nats!(go.client, [
             "--js-domain",
             "beam",
             "consumer",
             "next",
             "BEAMSTREAM",
             "C",
             "--raw"
           ]) =~ "one"

    # A Go mirror of the natsbeam stream, through natsbeam's domain API prefix.
    for {s, m} <- [{"bs.b", "two"}, {"bs.c", "three"}, {"bs.d", "four"}],
        do: nats!(go.client, ["pub", s, m])

    assert wait_until(
             fn -> nats(port, ["stream", "info", "BEAMSTREAM"]) |> elem(0) =~ "Messages: 4" end,
             10_000
           )

    # deleted before the mirror exists: the Go mirror must see the gap
    nats!(port, ["stream", "rmm", "BEAMSTREAM", "2", "-f"])

    deleted =
      nats!(port, ["stream", "info", "BEAMSTREAM", "--json"])
      |> Jason.decode!()
      |> get_in(["state", "num_deleted"])

    assert deleted == 1

    cfg = Path.join(System.tmp_dir!(), "gomirror-#{System.unique_integer([:positive])}.json")

    File.write!(
      cfg,
      Jason.encode!(%{
        "name" => "GOMIR",
        "storage" => "memory",
        "mirror" => %{"name" => "BEAMSTREAM", "external" => %{"api" => "$JS.beam.API"}}
      })
    )

    on_exit(fn -> File.rm(cfg) end)
    assert nats!(go.client, ["stream", "add", "GOMIR", "--config", cfg]) =~ "GOMIR was created"

    go_info = fn name ->
      case nats(go.client, ["stream", "info", name, "--json"]) do
        {out, 0} -> Jason.decode!(out)["state"]
        _ -> %{}
      end
    end

    assert wait_until(fn -> go_info.("GOMIR")["messages"] == 3 end, 15_000),
           inspect(go_info.("GOMIR"))

    assert %{"first_seq" => 1, "last_seq" => 4, "num_deleted" => 1} = go_info.("GOMIR")
    assert {_, code} = nats(go.client, ["stream", "get", "GOMIR", "2"])
    assert code != 0

    # nats-server mirrors only skip the gaps they see in delivered sequences:
    # removals of messages they already hold are not followed by Go itself
    # (stream.go processInboundMirrorMsg/skipMsgs). What natsbeam owes them is
    # a consistent origin: after a delete + purge here, new messages keep
    # flowing and a fresh Go mirror lands exactly on natsbeam's state.
    nats!(port, ["stream", "rmm", "BEAMSTREAM", "3", "-f"])
    nats!(port, ["stream", "purge", "BEAMSTREAM", "-f"])
    nats!(go.client, ["pub", "bs.e", "five"])

    assert wait_until(fn -> go_info.("GOMIR")["last_seq"] == 5 end, 15_000),
           inspect(go_info.("GOMIR"))

    File.write!(
      cfg,
      Jason.encode!(%{
        "name" => "GOMIR2",
        "storage" => "memory",
        "mirror" => %{"name" => "BEAMSTREAM", "external" => %{"api" => "$JS.beam.API"}}
      })
    )

    assert nats!(go.client, ["stream", "add", "GOMIR2", "--config", cfg]) =~ "GOMIR2 was created"

    assert wait_until(fn -> go_info.("GOMIR2")["messages"] == 1 end, 15_000),
           inspect(go_info.("GOMIR2"))

    assert %{"first_seq" => 5, "last_seq" => 5} = go_info.("GOMIR2")
    assert nats!(go.client, ["stream", "get", "GOMIR2", "5"]) =~ "five"
  end

  # ------------------------------------------------------------ system account

  @sys_hub """
  system_account: SYS
  accounts {
    SYS: { users: [{user: sys, password: sys}] }
    APP: { users: [{user: app, password: app}] }
  }
  """

  defp sys_nats(port, args), do: nats(port, ["--user", "sys", "--password", "sys" | args])

  defp beam_name, do: Natsbeam.Server.info().name

  defp assert_server_list!(port, names) do
    assert wait_until(
             fn ->
               {out, code} = sys_nats(port, ["server", "list", "#{length(names)}"])
               code == 0 and Enum.all?(names, &(out =~ &1))
             end,
             20_000
           ),
           elem(sys_nats(port, ["server", "list", "#{length(names)}"]), 0)
  end

  defp stop_on_exit(pid) do
    on_exit(fn -> DynamicSupervisor.terminate_child(Natsbeam.LinkSupervisor, pid) end)
    pid
  end

  @tag go_extra: @sys_hub
  test "a second leaf remote bound to the system account makes each side's tooling see the other",
       %{go: go, port: port} do
    # Our system account keeps its default name "$SYS"; the leaf binds it to
    # the hub's SYS (leaf account names need not match).
    assert Natsbeam.Config.get([:auth, :system_account]) == "$SYS"
    Natsbeam.add_user("sys", password: "sys", account: "$SYS")
    url = "nats-leaf://127.0.0.1:#{go.leaf}"

    {:ok, app} = Natsbeam.add_leaf_remote(url, account: "$G", user: "app", pass: "app")
    {:ok, sys} = Natsbeam.add_leaf_remote(url, account: "$SYS", user: "sys", pass: "sys")
    stop_on_exit(app) && stop_on_exit(sys)

    assert wait_until(
             fn ->
               Natsbeam.Link.status(app).connected and Natsbeam.Link.status(sys).connected
             end,
             10_000
           )

    # the hub sees one leaf per account
    assert Natsbeam.Link.status(sys).account == "$SYS"

    assert_server_list!(go.client, [go.name, beam_name()])

    {info, 0} = sys_nats(go.client, ["server", "info", beam_name()])
    assert info =~ Natsbeam.Server.info().id, info

    {js, 0} = sys_nats(go.client, ["server", "report", "jetstream", "2"])
    assert js =~ go.name and js =~ beam_name(), js

    # and our side lists the hub through the same leaf
    assert_server_list!(port, [go.name, beam_name()])
    {info, 0} = sys_nats(port, ["server", "info", go.name])
    assert info =~ go.name, info

    # $SYS stays on the system-account leaf: the app leaf never carries it
    refute Enum.any?(Map.keys(:sys.get_state(app).sent), fn {_, s, _} ->
             String.starts_with?(s, "$SYS.")
           end)

    log = File.read!(go.log)
    refute log =~ "out of order remote server update"
  end

  test "a Go spoke sharing its system account with a natsbeam hub", %{port: port} do
    lport = GoServer.free_port()
    {:ok, acceptor} = Natsbeam.start_leaf_listener(lport)
    on_exit(fn -> DynamicSupervisor.terminate_child(Natsbeam.AcceptorSupervisor, acceptor) end)
    Natsbeam.add_user("sys", password: "sys", account: "$SYS")

    # nats-server spoke: its system account SYS rides a remote that logs into
    # our system account ($SYS) as user sys.
    spoke =
      GoServer.start!(
        jetstream: false,
        extra: """
        system_account: SYS
        accounts { SYS: { users: [{user: sys, password: sys}] } }
        leafnodes {
          remotes: [{ url: "nats-leaf://sys:sys@127.0.0.1:#{lport}", account: SYS }]
        }
        """
      )

    assert wait_until(
             fn ->
               Enum.any?(
                 Natsbeam.links(),
                 &(&1.kind == :leaf and &1.account == "$SYS" and &1.connected)
               )
             end,
             15_000
           ),
           inspect(Natsbeam.links(), pretty: true)

    assert_server_list!(spoke.client, [spoke.name, beam_name()])
    assert_server_list!(port, [spoke.name, beam_name()])
    {info, 0} = sys_nats(spoke.client, ["server", "info", beam_name()])
    assert info =~ Natsbeam.Server.info().id, info
  end
end
