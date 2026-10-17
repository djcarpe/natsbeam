defmodule Natsbeam.JetStreamTest do
  @moduledoc "JetStream through the in-VM client: streams, consumers, KV-style flows, persistence."
  use ExUnit.Case, async: false
  import Natsbeam.TestHelpers
  alias Natsbeam.Protocol.Headers

  @acc "JS"

  setup_all do
    dir = Natsbeam.JetStream.store_dir()
    File.rm_rf!(dir)
    :ok
  end

  setup do
    reset_config!()
    {:ok, _} = Natsbeam.add_account(@acc, jetstream: true)
    for info <- Natsbeam.streams(@acc), do: api("STREAM.DELETE.#{info["config"]["name"]}", "")
    :ok
  end

  # -- helpers ------------------------------------------------------------

  defp api(op, body) when is_map(body), do: api(op, Jason.encode!(body))

  defp api(op, body) do
    {:ok, %{payload: p}} = Natsbeam.request("$JS.API." <> op, body, account: @acc, timeout: 3_000)
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

  # Pull one message batch for a consumer; returns list of {subject, headers, payload, ack_reply}.
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

  defp ack(reply, body \\ "+ACK"), do: Natsbeam.publish(reply, body, account: @acc)

  # -- tests --------------------------------------------------------------

  test "account info and stream lifecycle" do
    info = api("INFO", "")
    assert info["type"] == "io.nats.jetstream.api.v1.account_info_response"
    assert info["limits"]["max_streams"] == -1

    resp = mkstream("ORDERS", ["orders.>"])
    assert resp["type"] == "io.nats.jetstream.api.v1.stream_create_response"
    assert resp["config"]["retention"] == "limits"
    assert resp["config"]["max_msgs"] == -1
    assert resp["state"]["messages"] == 0

    # idempotent create with the same config, conflict with a different one
    assert mkstream("ORDERS", ["orders.>"])["config"]["name"] == "ORDERS"

    assert %{"error" => %{"err_code" => 10058}} =
             api("STREAM.CREATE.ORDERS", %{
               "name" => "ORDERS",
               "subjects" => ["other.>"],
               "storage" => "memory"
             })

    assert %{"error" => %{"err_code" => 10065}} =
             api("STREAM.CREATE.OTHER", %{
               "name" => "OTHER",
               "subjects" => ["orders.eu.>"],
               "storage" => "memory"
             })

    assert %{"streams" => ["ORDERS"], "total" => 1} = api("STREAM.NAMES", "")
    assert [%{"config" => %{"name" => "ORDERS"}}] = api("STREAM.LIST", "")["streams"]
    assert %{"error" => %{"err_code" => 10059}} = api("STREAM.INFO.NOPE", "")

    assert %{"success" => true} = api("STREAM.DELETE.ORDERS", "")
    assert %{"streams" => []} = api("STREAM.NAMES", "")
  end

  test "publishing stores messages with pubacks, dedupe and expected headers" do
    mkstream("ORDERS", ["orders.>"])
    assert %{"stream" => "ORDERS", "seq" => 1} = pub("orders.new", "one")
    assert %{"seq" => 2} = pub("orders.new", "two", [{"Nats-Msg-Id", "abc"}])

    assert %{"seq" => 2, "duplicate" => true} =
             pub("orders.new", "two again", [{"Nats-Msg-Id", "abc"}])

    assert %{"seq" => 3} = pub("orders.eu", "three", [{"Nats-Expected-Last-Sequence", "2"}])

    assert %{"error" => %{"err_code" => 10071, "description" => "wrong last sequence: 3"}} =
             pub("orders.eu", "x", [{"Nats-Expected-Last-Sequence", "2"}])

    assert %{"error" => %{"err_code" => 10060}} =
             pub("orders.eu", "x", [{"Nats-Expected-Stream", "OTHER"}])

    assert %{"seq" => 4} =
             pub("orders.eu", "four", [{"Nats-Expected-Last-Subject-Sequence", "3"}])

    assert %{"error" => %{"err_code" => 10071}} =
             pub("orders.us", "x", [{"Nats-Expected-Last-Subject-Sequence", "1"}])

    assert %{"seq" => 5} =
             pub("orders.us", "five", [{"Nats-Expected-Last-Subject-Sequence", "0"}])

    info = api("STREAM.INFO.ORDERS", %{"subjects_filter" => ">"})
    assert info["state"]["messages"] == 5
    assert info["state"]["num_subjects"] == 3
    assert info["state"]["subjects"] == %{"orders.new" => 2, "orders.eu" => 2, "orders.us" => 1}

    # get by seq, last by subject, next by subject
    assert %{
             "message" => %{"seq" => 2, "subject" => "orders.new", "data" => data, "hdrs" => hdrs}
           } = api("STREAM.MSG.GET.ORDERS", %{"seq" => 2})

    assert Base.decode64!(data) == "two"
    assert Base.decode64!(hdrs) =~ "Nats-Msg-Id: abc"

    assert %{"message" => %{"seq" => 4}} =
             api("STREAM.MSG.GET.ORDERS", %{"last_by_subj" => "orders.eu"})

    assert %{"message" => %{"seq" => 3}} =
             api("STREAM.MSG.GET.ORDERS", %{"next_by_subj" => "orders.eu", "seq" => 2})

    assert %{"error" => %{"err_code" => 10037}} = api("STREAM.MSG.GET.ORDERS", %{"seq" => 99})

    assert %{"success" => true} = api("STREAM.MSG.DELETE.ORDERS", %{"seq" => 3})
    assert %{"state" => %{"messages" => 4, "num_deleted" => 1}} = api("STREAM.INFO.ORDERS", "")

    assert %{"success" => true, "purged" => 2} =
             api("STREAM.PURGE.ORDERS", %{"filter" => "orders.new"})

    assert %{"success" => true, "purged" => 2} = api("STREAM.PURGE.ORDERS", "")
    assert %{"state" => %{"messages" => 0, "first_seq" => 6}} = api("STREAM.INFO.ORDERS", "")
  end

  test "limits: max_msgs, max_msgs_per_subject, discard new, max_msg_size" do
    mkstream("LIM", ["lim.>"], %{"max_msgs" => 3})
    for i <- 1..5, do: pub("lim.a", "#{i}")

    assert %{"state" => %{"messages" => 3, "first_seq" => 3, "last_seq" => 5}} =
             api("STREAM.INFO.LIM", "")

    mkstream("PERSUBJ", ["ps.>"], %{"max_msgs_per_subject" => 2})
    for i <- 1..3, do: pub("ps.a", "#{i}")
    pub("ps.b", "x")
    assert %{"state" => %{"messages" => 3}} = api("STREAM.INFO.PERSUBJ", "")

    assert %{"message" => %{"seq" => 2}} =
             api("STREAM.MSG.GET.PERSUBJ", %{"next_by_subj" => "ps.a", "seq" => 1})

    mkstream("NEW", ["new.>"], %{"max_msgs" => 1, "discard" => "new", "max_msg_size" => 5})
    assert %{"seq" => 1} = pub("new.a", "ok")

    assert %{"error" => %{"err_code" => 10077, "description" => "maximum messages exceeded"}} =
             pub("new.a", "no")

    api("STREAM.PURGE.NEW", "")
    assert %{"error" => %{"err_code" => 10054}} = pub("new.a", "too large")
  end

  test "pull consumer: fetch, ack, redelivery after ack_wait, nak, term, info" do
    mkstream("WORK", ["work.>"])
    for i <- 1..4, do: pub("work.q", "job #{i}")

    cons =
      mkconsumer("WORK", %{
        "durable_name" => "W",
        "ack_policy" => "explicit",
        "ack_wait" => 300_000_000,
        "filter_subject" => "work.>"
      })

    assert cons["config"]["max_waiting"] == 512
    assert cons["num_pending"] == 4

    [{"work.q", nil, "job 1", r1}, {"work.q", nil, "job 2", r2}] = fetch("WORK", "W", 2)
    assert r1 =~ ~r/^\$JS\.ACK\.WORK\.W\.1\.1\.1\.\d+\.3$/
    ack(r1)
    ack(r2, "-NAK")

    # job 2 comes back immediately after the NAK
    assert [{_, _, "job 2", r2b}] = fetch("WORK", "W", 1)
    assert r2b =~ ~r/^\$JS\.ACK\.WORK\.W\.2\.2\./
    ack(r2b, "+TERM")

    # job 3 delivered but never acked: redelivered after ack_wait
    assert [{_, _, "job 3", _}] = fetch("WORK", "W", 1)
    Process.sleep(600)
    assert [{_, _, "job 3", r3}] = fetch("WORK", "W", 1)
    ack(r3)
    assert [{_, _, "job 4", r4}] = fetch("WORK", "W", 1)
    ack(r4)

    info = api("CONSUMER.INFO.WORK.W", "")
    assert info["num_ack_pending"] == 0
    assert info["num_pending"] == 0
    assert info["num_redelivered"] == 2
    assert info["ack_floor"]["stream_seq"] == 4
    assert %{"consumers" => ["W"]} = api("CONSUMER.NAMES.WORK", "")

    # nothing left: no_wait yields a 404 status, expiry yields 408
    assert [{_, hdrs, "", nil}] = fetch("WORK", "W", 1, %{"no_wait" => true})
    assert {:ok, %{status: "404"}} = Headers.decode(hdrs)
    assert [{_, hdrs, "", nil}] = fetch("WORK", "W", 1, %{"expires" => 200_000_000})

    assert {:ok, %{status: "408", fields: [{"Nats-Pending-Messages", "1"}]}} =
             Headers.decode(hdrs)

    assert %{"success" => true} = api("CONSUMER.DELETE.WORK.W", "")
    assert %{"error" => %{"err_code" => 10014}} = api("CONSUMER.INFO.WORK.W", "")
  end

  test "push consumer delivers to an in-VM subscriber with original subjects and heartbeats" do
    mkstream("EVENTS", ["ev.>"])
    pub("ev.a", "1")
    pub("ev.b", "2")
    {:ok, sid} = Natsbeam.subscribe("deliver.ev", account: @acc)

    mkconsumer("EVENTS", %{
      "durable_name" => "P",
      "deliver_subject" => "deliver.ev",
      "ack_policy" => "none",
      "idle_heartbeat" => 200_000_000
    })

    assert_receive {:natsbeam_msg, ^sid, "ev.a", "$JS.ACK.EVENTS.P.1.1.1." <> _, nil, "1"}, 2_000
    assert_receive {:natsbeam_msg, ^sid, "ev.b", "$JS.ACK.EVENTS.P.1.2.2." <> _, nil, "2"}, 2_000
    pub("ev.c", "3")
    assert_receive {:natsbeam_msg, ^sid, "ev.c", _, nil, "3"}, 2_000
    assert_receive {:natsbeam_msg, ^sid, "deliver.ev", nil, hb, ""}, 2_000
    assert {:ok, %{status: "100", description: "Idle Heartbeat"} = h} = Headers.decode(hb)
    assert Headers.get(h, "Nats-Last-Stream") == "3"
    assert api("CONSUMER.INFO.EVENTS.P", "")["push_bound"] == true
  end

  test "deliver policies and ordered-consumer style last_per_subject" do
    mkstream("DP", ["dp.>"])
    for {s, v} <- [{"dp.a", "a1"}, {"dp.b", "b1"}, {"dp.a", "a2"}], do: pub(s, v)

    mkconsumer("DP", %{"name" => "last", "deliver_policy" => "last", "ack_policy" => "none"})
    assert [{_, _, "a2", _}] = fetch("DP", "last", 5)

    mkconsumer("DP", %{
      "name" => "lps",
      "deliver_policy" => "last_per_subject",
      "ack_policy" => "none"
    })

    assert [{_, _, "b1", _}, {_, _, "a2", _}] = fetch("DP", "lps", 5)

    mkconsumer("DP", %{
      "name" => "seq",
      "deliver_policy" => "by_start_sequence",
      "opt_start_seq" => 2,
      "ack_policy" => "none"
    })

    assert [{_, _, "b1", _}, {_, _, "a2", _}] = fetch("DP", "seq", 5)

    mkconsumer("DP", %{"name" => "new", "deliver_policy" => "new", "ack_policy" => "none"})

    assert [] =
             fetch("DP", "new", 5, %{"no_wait" => false, "expires" => 100_000_000})
             |> Enum.reject(&match?({_, _, "", nil}, &1))

    pub("dp.c", "c1")
    assert [{_, _, "c1", _}] = fetch("DP", "new", 5)

    mkconsumer("DP", %{
      "name" => "filt",
      "filter_subjects" => ["dp.a", "dp.c"],
      "ack_policy" => "none"
    })

    assert [{_, _, "a1", _}, {_, _, "a2", _}, {_, _, "c1", _}] = fetch("DP", "filt", 10)

    assert %{"error" => %{"err_code" => 10093}} =
             api("CONSUMER.CREATE.DP.bad", %{
               "stream_name" => "DP",
               "config" => %{"name" => "bad", "filter_subject" => "other.x"}
             })
  end

  test "workqueue and interest retention remove acked messages" do
    mkstream("WQ", ["wq.>"], %{"retention" => "workqueue"})
    pub("wq.a", "1")
    pub("wq.a", "2")
    mkconsumer("WQ", %{"durable_name" => "C"})

    assert %{"error" => %{"err_code" => 10099}} =
             api("CONSUMER.CREATE.WQ.D", %{
               "stream_name" => "WQ",
               "config" => %{"durable_name" => "D"}
             })

    assert %{"error" => %{"err_code" => 10100}} =
             api("CONSUMER.CREATE.WQ.E", %{
               "stream_name" => "WQ",
               "config" => %{"durable_name" => "E", "filter_subject" => "wq.a"}
             })

    [{_, _, "1", r}] = fetch("WQ", "C", 1)
    ack(r)
    assert wait_until(fn -> api("STREAM.INFO.WQ", "")["state"]["messages"] == 1 end)

    mkstream("INT", ["int.>"], %{"retention" => "interest"})
    assert %{"seq" => 1} = pub("int.a", "nobody cares")
    assert %{"state" => %{"messages" => 0}} = api("STREAM.INFO.INT", "")
    mkconsumer("INT", %{"durable_name" => "A"})
    mkconsumer("INT", %{"durable_name" => "B"})
    pub("int.a", "kept")
    assert %{"state" => %{"messages" => 1}} = api("STREAM.INFO.INT", "")
    [{_, _, "kept", ra}] = fetch("INT", "A", 1)
    ack(ra)
    assert %{"state" => %{"messages" => 1}} = api("STREAM.INFO.INT", "")
    [{_, _, "kept", rb}] = fetch("INT", "B", 1)
    ack(rb)
    assert wait_until(fn -> api("STREAM.INFO.INT", "")["state"]["messages"] == 0 end)
  end

  test "KV-style operations: rollup, direct get, CAS" do
    mkstream("KV_CFG", ["$KV.CFG.>"], %{
      "max_msgs_per_subject" => 3,
      "allow_rollup_hdrs" => true,
      "allow_direct" => true,
      "deny_delete" => true
    })

    pub("$KV.CFG.color", "blue")
    pub("$KV.CFG.color", "red")
    pub("$KV.CFG.size", "L")

    # direct get last by subject returns the raw message with JS headers
    {:ok, %{headers: h, payload: "red"}} =
      Natsbeam.request("$JS.API.DIRECT.GET.KV_CFG.$KV.CFG.color", "", account: @acc)

    assert Headers.get(h, "Nats-Sequence") == "2"
    assert Headers.get(h, "Nats-Subject") == "$KV.CFG.color"

    {:ok, %{headers: h404}} =
      Natsbeam.request("$JS.API.DIRECT.GET.KV_CFG.$KV.CFG.nope", "", account: @acc)

    assert h404.status == "404"

    {:ok, %{payload: "L"}} =
      Natsbeam.request(
        "$JS.API.DIRECT.GET.KV_CFG",
        Jason.encode!(%{"last_by_subj" => "$KV.CFG.size"}),
        account: @acc
      )

    # CAS: create only when key absent / update only at revision
    assert %{"error" => %{"err_code" => 10071}} =
             pub("$KV.CFG.color", "green", [{"Nats-Expected-Last-Subject-Sequence", "0"}])

    assert %{"seq" => 4} =
             pub("$KV.CFG.color", "green", [{"Nats-Expected-Last-Subject-Sequence", "2"}])

    # rollup purges history for the subject
    assert %{"seq" => 5} =
             pub("$KV.CFG.color", "", [{"Nats-Rollup", "sub"}, {"KV-Operation", "PURGE"}])

    assert %{"state" => %{"messages" => 2}} = api("STREAM.INFO.KV_CFG", "")
    assert %{"error" => %{"err_code" => 10057}} = api("STREAM.MSG.DELETE.KV_CFG", %{"seq" => 5})
  end

  test "file streams and durable consumers survive a restart" do
    mkstream("DUR", ["dur.>"], %{"storage" => "file"})
    for i <- 1..3, do: pub("dur.x", "m#{i}")
    mkconsumer("DUR", %{"durable_name" => "D"})
    [{_, _, "m1", r}] = fetch("DUR", "D", 1)
    ack(r)
    [{_, _, "m2", _}] = fetch("DUR", "D", 1)

    # Simulate a server restart of the JetStream subsystem.
    Supervisor.terminate_child(Natsbeam.Supervisor, Natsbeam.JetStream)
    Supervisor.restart_child(Natsbeam.Supervisor, Natsbeam.JetStream)

    assert wait_until(fn ->
             match?({:ok, _}, Natsbeam.JetStream.Consumer.lookup(@acc, "DUR", "D"))
           end)

    assert %{"state" => %{"messages" => 3, "last_seq" => 3}} = api("STREAM.INFO.DUR", "")
    info = api("CONSUMER.INFO.DUR.D", "")
    assert info["ack_floor"]["stream_seq"] == 1
    assert info["num_ack_pending"] == 1
    # the unacked m2 is redelivered, then m3
    assert [{_, _, "m2", _}, {_, _, "m3", _}] = fetch("DUR", "D", 2)
    api("STREAM.DELETE.DUR", "")
    refute File.exists?(Path.join([Natsbeam.JetStream.store_dir(), @acc, "streams", "DUR"]))
  end

  test "accounts without jetstream get no API responder" do
    Natsbeam.add_account("PLAIN", jetstream: false)
    assert {:error, :no_responders} = Natsbeam.request("$JS.API.INFO", "", account: "PLAIN")
    Natsbeam.add_account("PLAIN", jetstream: true)

    assert wait_until(fn ->
             match?({:ok, _}, Natsbeam.request("$JS.API.INFO", "", account: "PLAIN"))
           end)
  end
end
