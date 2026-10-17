defmodule Natsbeam.JetStreamFeaturesTest do
  @moduledoc "Per-message TTLs, consumer pause, mirrors/sources, snapshots."
  use ExUnit.Case, async: false
  import Natsbeam.TestHelpers
  alias Natsbeam.Protocol.Headers

  @acc "JSF"

  setup do
    reset_config!()
    {:ok, _} = Natsbeam.add_account(@acc, jetstream: true)
    for info <- Natsbeam.streams(@acc), do: api("STREAM.DELETE.#{info["config"]["name"]}", "")
    :ok
  end

  defp api(op, body) when is_map(body), do: api(op, Jason.encode!(body))

  defp api(op, body) do
    {:ok, %{payload: p}} = Natsbeam.request("$JS.API." <> op, body, account: @acc, timeout: 5_000)
    Jason.decode!(p)
  end

  defp pub(subject, data, headers \\ nil) do
    opts = [account: @acc, timeout: 3_000]
    opts = if headers, do: Keyword.put(opts, :headers, headers), else: opts
    {:ok, %{payload: p}} = Natsbeam.request(subject, data, opts)
    Jason.decode!(p)
  end

  defp mkstream(name, subjects, extra \\ %{}) do
    resp =
      api(
        "STREAM.CREATE.#{name}",
        Map.merge(%{"name" => name, "subjects" => subjects, "storage" => "memory"}, extra)
      )

    refute Map.has_key?(resp, "error"), inspect(resp)
    resp
  end

  defp mkconsumer(stream, cfg) do
    resp =
      api("CONSUMER.CREATE.#{stream}.#{cfg["name"] || cfg["durable_name"]}", %{
        "stream_name" => stream,
        "config" => cfg
      })

    refute Map.has_key?(resp, "error"), inspect(resp)
    resp
  end

  defp fetch(stream, consumer, batch, opts \\ []) do
    inbox = "_INBOX.#{System.unique_integer([:positive])}"
    {:ok, sid} = Natsbeam.subscribe(inbox, account: @acc)
    body = Map.merge(%{"batch" => batch, "expires" => 500_000_000}, Map.new(opts))

    {:ok, _} =
      Natsbeam.publish("$JS.API.CONSUMER.MSG.NEXT.#{stream}.#{consumer}", Jason.encode!(body),
        account: @acc,
        reply: inbox
      )

    msgs = collect_msgs(sid, [], 1_000)
    Natsbeam.unsubscribe(sid, account: @acc)
    msgs
  end

  defp collect_msgs(sid, acc, timeout) do
    receive do
      {:natsbeam_msg, ^sid, subject, reply, headers, payload} ->
        collect_msgs(sid, [{subject, headers, payload, reply} | acc], 150)
    after
      timeout -> Enum.reverse(acc)
    end
  end

  defp messages(stream), do: api("STREAM.INFO.#{stream}", "")["state"]["messages"]

  # -- per-message TTL --------------------------------------------------

  test "per-message TTLs expire messages, never exempts from max_age, markers are left" do
    assert %{"error" => %{"err_code" => 10166}} =
             mkstream("NOTTL", ["nottl.>"]) && pub("nottl.a", "x", [{"Nats-TTL", "1s"}])

    mkstream("TTL", ["ttl.>"], %{
      "allow_msg_ttl" => true,
      "max_age" => 1_500_000_000,
      "subject_delete_marker_ttl" => 2_000_000_000
    })

    assert %{"error" => %{"err_code" => 10165}} = pub("ttl.a", "x", [{"Nats-TTL", "soon"}])
    assert %{"seq" => 1} = pub("ttl.short", "x", [{"Nats-TTL", "1s"}])
    assert %{"seq" => 2} = pub("ttl.forever", "x", [{"Nats-TTL", "never"}])
    assert %{"seq" => 3} = pub("ttl.plain", "x")
    assert messages("TTL") == 3

    # after ~1.2s the 1s TTL message is gone and a delete marker took its place
    assert wait_until(fn -> api("STREAM.MSG.GET.TTL", %{"seq" => 1})["error"] != nil end, 2_500)

    assert %{"message" => %{"subject" => "ttl.short", "hdrs" => hdrs}} =
             api("STREAM.MSG.GET.TTL", %{"last_by_subj" => "ttl.short"})

    assert {:ok, h} = Headers.decode(Base.decode64!(hdrs))
    assert Headers.get(h, "Nats-Marker-Reason") == "MaxAge"
    assert Headers.get(h, "Nats-TTL") == "2s"

    # max_age (1.5s) removes the plain message but not the "never" one
    assert wait_until(fn -> api("STREAM.MSG.GET.TTL", %{"seq" => 3})["error"] != nil end, 3_000)
    assert %{"message" => %{"seq" => 2}} = api("STREAM.MSG.GET.TTL", %{"seq" => 2})
    # and the marker itself expires without leaving another marker
    assert wait_until(fn -> messages("TTL") == 1 end, 4_000)
  end

  # -- consumer pause ---------------------------------------------------

  test "consumer pause stops delivery until pause_until" do
    mkstream("PAUSE", ["pause.>"])
    pub("pause.a", "1")
    mkconsumer("PAUSE", %{"durable_name" => "P"})

    until = DateTime.utc_now() |> DateTime.add(700, :millisecond) |> DateTime.to_iso8601()

    assert %{"paused" => true, "pause_remaining" => rem} =
             api("CONSUMER.PAUSE.PAUSE.P", %{"pause_until" => until})

    assert rem > 0
    assert api("CONSUMER.INFO.PAUSE.P", "")["paused"] == true

    assert [] =
             fetch("PAUSE", "P", 1, %{"expires" => 200_000_000})
             |> Enum.reject(&match?({_, _, "", nil}, &1))

    assert wait_until(fn -> api("CONSUMER.INFO.PAUSE.P", "")["paused"] == false end, 2_000)
    assert [{_, _, "1", _}] = fetch("PAUSE", "P", 1)

    # unpause explicitly
    api("CONSUMER.PAUSE.PAUSE.P", %{
      "pause_until" => DateTime.utc_now() |> DateTime.add(60, :second) |> DateTime.to_iso8601()
    })

    assert %{"paused" => false} = api("CONSUMER.PAUSE.PAUSE.P", %{})
  end

  # -- mirrors and sources ----------------------------------------------

  test "mirrors copy the origin with the same sequences and follow deletes" do
    mkstream("ORIG", ["orig.>"])
    for i <- 1..3, do: pub("orig.a", "m#{i}")
    api("STREAM.MSG.DELETE.ORIG", %{"seq" => 2})

    assert %{"error" => %{"err_code" => 10033}} =
             api("STREAM.CREATE.BADM", %{
               "name" => "BADM",
               "subjects" => ["x"],
               "mirror" => %{"name" => "ORIG"},
               "storage" => "memory"
             })

    assert %{"error" => %{"err_code" => 10052}} =
             api("STREAM.CREATE.BADM", %{
               "name" => "BADM",
               "mirror" => %{"name" => "NOPE"},
               "storage" => "memory"
             })

    resp = mkstream("MIR", [], %{"mirror" => %{"name" => "ORIG"}})
    assert resp["config"]["mirror"]["name"] == "ORIG"
    assert wait_until(fn -> messages("MIR") == 2 end)
    info = api("STREAM.INFO.MIR", "")
    assert info["state"]["last_seq"] == 3
    assert info["mirror"]["name"] == "ORIG" and info["mirror"]["lag"] == 0

    assert %{"message" => %{"seq" => 3, "subject" => "orig.a"}} =
             api("STREAM.MSG.GET.MIR", %{"seq" => 3})

    # live updates and deletes flow through; mirrors reject direct publishes
    pub("orig.b", "m4")
    assert wait_until(fn -> messages("MIR") == 3 end)
    api("STREAM.MSG.DELETE.ORIG", %{"seq" => 1})
    assert wait_until(fn -> messages("MIR") == 2 end)
    # mirrors have no subjects of their own: publishing never reaches them
    assert api("STREAM.INFO.MIR", "")["config"]["subjects"] in [nil, []]

    # consumers read from the mirror
    mkconsumer("MIR", %{"durable_name" => "C", "ack_policy" => "none"})
    assert [{"orig.a", _, "m3", _}, {"orig.b", _, "m4", _}] = fetch("MIR", "C", 5)
  end

  # natsbeam has no account imports/exports, so `external.api` cannot reach
  # another local account; it can still address this account's own API
  # prefix, which runs the same ExternalSource path (push consumer through the
  # API, STREAM.INFO polling) as an origin in another domain over a link.
  test "external mirrors (through an API prefix) follow deletes and purges" do
    mkstream("XORIG", ["xorig.>"])
    for i <- 1..4, do: pub("xorig.a", "m#{i}")
    # deleted before the mirror exists: never delivered, a gap in the mirror
    api("STREAM.MSG.DELETE.XORIG", %{"seq" => 2})

    mkstream("XMIR", [], %{
      "mirror" => %{"name" => "XORIG", "external" => %{"api" => "$JS.API"}}
    })

    assert wait_until(fn -> messages("XMIR") == 3 end, 5_000)
    assert api("STREAM.INFO.XMIR", "")["state"]["last_seq"] == 4

    # deleted on the origin after it was mirrored
    api("STREAM.MSG.DELETE.XORIG", %{"seq" => 3})
    assert wait_until(fn -> messages("XMIR") == 2 end, 6_000)
    assert %{"error" => _} = api("STREAM.MSG.GET.XMIR", %{"seq" => 3})
    assert %{"message" => %{"seq" => 4}} = api("STREAM.MSG.GET.XMIR", %{"seq" => 4})

    info = api("STREAM.INFO.XMIR", %{"deleted_details" => true})
    assert info["state"]["deleted"] == [2, 3]

    # new messages keep flowing after the delete
    pub("xorig.b", "m5")
    assert wait_until(fn -> messages("XMIR") == 3 end, 5_000)

    # purge on the origin empties the mirror; later messages still arrive
    api("STREAM.PURGE.XORIG", "")
    assert wait_until(fn -> messages("XMIR") == 0 end, 6_000)
    assert api("STREAM.INFO.XMIR", "")["state"]["first_seq"] == 6

    pub("xorig.c", "m6")
    assert wait_until(fn -> messages("XMIR") == 1 end, 5_000)

    assert %{"message" => %{"seq" => 6, "subject" => "xorig.c"}} =
             api("STREAM.MSG.GET.XMIR", %{"seq" => 6})
  end

  test "sources aggregate several streams and tag messages" do
    mkstream("S1", ["s1.>"])
    mkstream("S2", ["s2.>"])
    pub("s1.a", "one")
    pub("s2.a", "two")

    mkstream("AGG", ["agg.>"], %{
      "sources" => [%{"name" => "S1"}, %{"name" => "S2", "filter_subject" => "s2.a"}]
    })

    assert wait_until(fn -> messages("AGG") == 2 end)
    pub("s2.other", "ignored")
    pub("s2.a", "three")
    pub("agg.own", "mine")
    assert wait_until(fn -> messages("AGG") == 4 end)

    info = api("STREAM.INFO.AGG", "")
    assert [%{"name" => "S1"}, %{"name" => "S2", "filter_subject" => "s2.a"}] = info["sources"]

    mkconsumer("AGG", %{"durable_name" => "C", "ack_policy" => "none"})
    msgs = fetch("AGG", "C", 10)

    assert Enum.map(msgs, fn {_, _, p, _} -> p end) |> Enum.sort() == [
             "mine",
             "one",
             "three",
             "two"
           ]

    {_, hdrs, "one", _} = Enum.find(msgs, &match?({_, _, "one", _}, &1))
    assert {:ok, h} = Headers.decode(hdrs)
    assert Headers.get(h, "Nats-Stream-Source") =~ ~r/^S1 1/
  end

  # -- snapshots ----------------------------------------------------------

  test "snapshot and restore round-trip through the API" do
    mkstream("SNAP", ["snap.>"], %{"storage" => "file"})
    for i <- 1..5, do: pub("snap.a", "m#{i}")
    mkconsumer("SNAP", %{"durable_name" => "D"})
    [{_, _, "m1", r}] = fetch("SNAP", "D", 1)
    Natsbeam.publish(r, "+ACK", account: @acc)

    # snapshot: chunks arrive on a deliver subject, empty message ends it
    deliver = "_INBOX.snap.#{System.unique_integer([:positive])}"
    {:ok, sid} = Natsbeam.subscribe(deliver, account: @acc)
    resp = api("STREAM.SNAPSHOT.SNAP", %{"deliver_subject" => deliver, "chunk_size" => 1024})
    assert resp["config"]["name"] == "SNAP" and resp["state"]["messages"] == 5
    archive = collect_chunks(sid, [])
    assert byte_size(archive) > 0

    api("STREAM.DELETE.SNAP", "")
    assert %{"error" => _} = api("STREAM.INFO.SNAP", "")

    # restore: request gives a deliver subject; send chunks as requests, then an empty one
    %{"deliver_subject" => rsubj} =
      api("STREAM.RESTORE.SNAP", %{"config" => resp["config"], "state" => resp["state"]})

    for <<chunk::binary-size(1000) <- archive>> do
      {:ok, %{payload: ""}} = Natsbeam.request(rsubj, chunk, account: @acc)
    end

    tail =
      binary_part(
        archive,
        byte_size(archive) - rem(byte_size(archive), 1000),
        rem(byte_size(archive), 1000)
      )

    if tail != "", do: {:ok, _} = Natsbeam.request(rsubj, tail, account: @acc)
    {:ok, %{payload: final}} = Natsbeam.request(rsubj, "", account: @acc)

    assert %{
             "type" => "io.nats.jetstream.api.v1.stream_create_response",
             "state" => %{"messages" => 5}
           } = Jason.decode!(final)

    assert wait_until(fn -> match?(%{"name" => "D"}, api("CONSUMER.INFO.SNAP.D", "")) end)
    assert api("CONSUMER.INFO.SNAP.D", "")["ack_floor"]["stream_seq"] == 1
    assert [{_, _, "m2", _}] = fetch("SNAP", "D", 1)

    assert %{"error" => %{"err_code" => 10058}} =
             api("STREAM.RESTORE.SNAP", %{"config" => resp["config"]})
  end

  defp collect_chunks(sid, acc) do
    receive do
      {:natsbeam_msg, ^sid, _, reply, _, ""} ->
        _ = reply
        IO.iodata_to_binary(Enum.reverse(acc))

      {:natsbeam_msg, ^sid, _, reply, _, chunk} ->
        if reply, do: Natsbeam.publish(reply, "", account: @acc)
        collect_chunks(sid, [chunk | acc])
    after
      5_000 -> IO.iodata_to_binary(Enum.reverse(acc))
    end
  end

  test "stepdown without replicas is refused" do
    mkstream("SOLO", ["solo.>"])
    assert %{"error" => %{"code" => 400}} = api("STREAM.LEADER.STEPDOWN.SOLO", "")
    assert %{"success" => true} = api("META.LEADER.STEPDOWN", "")
  end
end
