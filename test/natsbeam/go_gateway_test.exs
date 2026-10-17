defmodule Natsbeam.GoGatewayTest do
  @moduledoc """
  natsbeam as one cluster ("beam") of a super-cluster with a real Go
  nats-server cluster ("gocluster") over the gateway protocol.
  """
  use ExUnit.Case, async: false
  import Natsbeam.TestHelpers
  alias Natsbeam.GoServer
  @moduletag :go
  @moduletag timeout: 180_000

  setup context do
    reset_config!()
    Natsbeam.Config.put(:cluster_name, "beam")

    if context[:optimistic] do
      Natsbeam.Config.put(:gateway_interest_only, false)
      Natsbeam.Config.put(:gateway_max_runsub, 3)
    end

    {:ok, acceptor} = Natsbeam.start_gateway_listener(0, host: "127.0.0.1")
    {:ok, gw_port} = Natsbeam.Link.Acceptor.port(:gateway)

    on_exit(fn ->
      DynamicSupervisor.terminate_child(Natsbeam.AcceptorSupervisor, acceptor)
      Natsbeam.remove_gateway("gocluster")
    end)

    go =
      GoServer.start!(
        jetstream: false,
        gateway: [name: "gocluster", gateways: [{"beam", "nats://127.0.0.1:#{gw_port}"}]]
      )

    port = start_listener!()
    # natsbeam accounts start lazily; have the default one up before Go sends interest
    {:ok, _, _} = Natsbeam.Account.ensure("$G")
    {:ok, out} = Natsbeam.add_gateway("nats://127.0.0.1:#{go.gateway}", name: "gocluster")
    assert wait_until(fn -> connected?(:outbound) and connected?(:inbound) end, 15_000)
    {:ok, go: go, port: port, out: out, gw_port: gw_port}
  end

  defp connected?(role) do
    Enum.any?(
      Natsbeam.gateways(),
      &(&1.role == role and &1.connected and &1.gateway == "gocluster")
    )
  end

  defp status(role), do: Enum.find(Natsbeam.gateways(), &(&1.role == role and &1.connected))

  defp received(out), do: length(String.split(out, "Received")) - 1

  # Retry a Go-side publish until the natsbeam subscriber sees it (interest
  # propagation across the gateway is asynchronous).
  defp go_pub_until(go, sid, subject, payload, extra \\ []) do
    wait_until(
      fn ->
        nats(go.client, ["pub", subject, payload | extra])

        receive do
          {:natsbeam_msg, ^sid, ^subject, _, _, ^payload} -> true
        after
          300 -> false
        end
      end,
      10_000
    )
  end

  test "both connections come up and are visible on both sides", %{go: go} do
    out = status(:outbound)
    inb = status(:inbound)
    assert out.remote_name == go.name and inb.remote_name == go.name
    assert out.kind == :gateway and out.remote_cluster == "gocluster"
    # Go >= 2.9 announces interest-only mode and switches its accounts.
    assert out.interest_only
    assert wait_until(fn -> status(:outbound).modes["$G"] == :interest_only end, 5_000)
    # accounts are created lazily; a new one is switched as it starts
    {:ok, _} = Natsbeam.subscribe("any.thing")
    assert wait_until(fn -> status(:inbound).modes["$G"] == :interest_only end, 5_000)

    log = File.read!(go.log)
    assert log =~ ~s(Outbound gateway connection to "beam")
    assert log =~ ~s(Inbound gateway connection from "beam")
    assert Enum.count(Natsbeam.links(), &(&1.kind == :gateway)) == 2
  end

  test "pub/sub with headers both ways", %{go: go, port: port} do
    # Go publisher -> natsbeam subscriber
    {:ok, sid} = Natsbeam.subscribe("g2b.>")
    assert go_pub_until(go, sid, "g2b.1", "from go", ["-H", "X-Go:1"])

    nats!(go.client, ["pub", "g2b.2", "hdr", "-H", "X-Go:2"])
    assert_receive {:natsbeam_msg, ^sid, "g2b.2", nil, hdrs, "hdr"}, 3_000
    assert hdrs =~ "X-Go: 2"

    # natsbeam publisher -> Go subscriber
    sub = nats_bg(go.client, ["sub", "--count", "100", "b2g.>"])
    collect(sub, ~r/Subscribing/)

    assert wait_until(
             fn ->
               nats(port, ["pub", "b2g.1", "hello", "-H", "X-Beam:1"])
               collect(sub, ~r/hello/, 300) =~ "hello"
             end,
             10_000
           )

    # in-VM publisher with headers; this subscriber leaves after one message
    vm = nats_bg(go.client, ["sub", "--count", "1", "b2vm.x"])
    collect(vm, ~r/Subscribing/)

    reg? = fn key ->
      reg = :sys.get_state(status(:outbound).pid).reg["$G"] || MapSet.new()
      MapSet.member?(reg, key)
    end

    assert wait_until(fn -> reg?.({:sub, "b2vm.x", nil}) end, 10_000)
    Natsbeam.publish("b2vm.x", "in-vm", headers: [{"X-Beam", "2"}])
    out = collect(vm, ~r/in-vm/, 8_000)

    assert out =~ "in-vm" and out =~ "X-Beam: 2",
           out <>
             (File.read!(go.log)
              |> String.split("\n")
              |> Enum.filter(&(&1 =~ "b2vm"))
              |> Enum.join("\n"))

    # Its RS- removes our gateway subscription again.
    assert wait_until(fn -> not reg?.({:sub, "b2vm.x", nil}) end, 10_000)
  end

  test "request/reply both ways across the gateway", %{go: go, port: port} do
    # natsbeam responder, Go requester
    responder =
      spawn_link(fn ->
        {:ok, s} = Natsbeam.subscribe("svc.beam")
        send(self(), :ready)

        loop = fn loop ->
          receive do
            {:natsbeam_msg, ^s, _, reply, _, p} ->
              Natsbeam.publish(reply, "beam:" <> p)
              loop.(loop)
          end
        end

        loop.(loop)
      end)

    _ = responder

    assert wait_until(
             fn ->
               case nats(go.client, ["request", "svc.beam", "ping", "--timeout", "1s"]) do
                 {out, 0} -> out =~ "beam:ping"
                 _ -> false
               end
             end,
             10_000
           )

    # Several in a row (fresh inbox each time: exercises the _GR_ reply mapping)
    for i <- 1..5 do
      {out, 0} = nats(go.client, ["request", "svc.beam", "n#{i}", "--timeout", "2s"])
      assert out =~ "beam:n#{i}"
    end

    # Go responder, natsbeam requester (CLI client and in-VM)
    rep = nats_bg(go.client, ["reply", "svc.go", "go-pong"])
    collect(rep, ~r/Listening/, 3_000)

    assert wait_until(
             fn ->
               case nats(port, ["request", "svc.go", "ping", "--timeout", "1s"]) do
                 {out, 0} -> out =~ "go-pong"
                 _ -> false
               end
             end,
             10_000
           )

    for _ <- 1..5 do
      assert {:ok, %{payload: "go-pong"}} = Natsbeam.request("svc.go", "x", timeout: 2_000)
    end

    # Fresh inboxes are recent subscriptions, so replies were mapped both ways:
    # Go sent us `_GR_.<gocluster hash>...` replies, we sent it ours, and the
    # responses came back as `_GR_.`-prefixed subjects to the origin cluster.
    log = File.read!(go.log)
    beam_hash = Natsbeam.Gateway.hash("beam")
    go_hash = Natsbeam.Gateway.hash("gocluster")
    assert log =~ "->> [RMSG $G svc.beam _GR_.#{go_hash}."
    assert log =~ "<<- [RMSG $G _GR_.#{go_hash}."
    # (`nats reply` is a queue subscriber: the `+ <reply> <queue>` form)
    assert log =~ "<<- [RMSG $G svc.go + _GR_.#{beam_hash}."
    assert log =~ "->> [RMSG $G _GR_.#{beam_hash}."
    assert status(:outbound).stats.out_msgs >= 5
    assert status(:inbound).stats.in_msgs >= 5
  end

  test "queue groups: local members first, remote group when none here", %{
    go: go,
    port: port
  } do
    # Only Go has members of "workers": every message crosses once.
    q1 = nats_bg(go.client, ["sub", "--queue", "workers", "--count", "100", "jobs.>"])
    q2 = nats_bg(go.client, ["sub", "--queue", "workers", "--count", "100", "jobs.>"])
    collect(q1, ~r/Subscribing/)
    collect(q2, ~r/Subscribing/)

    assert wait_until(
             fn ->
               st = :sys.get_state(status(:outbound).pid)
               match?(%{qsubs: n} when n > 0, st.out["$G"])
             end,
             10_000
           )

    for i <- 1..10, do: Natsbeam.publish("jobs.a", "j#{i}")
    Process.sleep(1_000)
    o1 = collect(q1, ~r/never/, 300)
    o2 = collect(q2, ~r/never/, 300)
    assert received(o1) + received(o2) == 10, o1 <> o2

    # natsbeam joins the group: natsbeam publishes stay here (one copy each),
    # Go publishes stay in Go.
    {:ok, qsid} = Natsbeam.subscribe("jobs.>", queue: "workers")
    Process.sleep(500)
    for i <- 1..6, do: nats!(port, ["pub", "jobs.b", "b#{i}"])

    local =
      Stream.repeatedly(fn ->
        receive do
          {:natsbeam_msg, ^qsid, _, _, _, _} -> 1
        after
          500 -> nil
        end
      end)
      |> Enum.take_while(& &1)
      |> length()

    assert local == 6
    assert received(collect(q1, ~r/never/, 300) <> collect(q2, ~r/never/, 300)) == 0

    for i <- 1..4, do: nats!(go.client, ["pub", "jobs.c", "c#{i}"])
    Process.sleep(800)
    refute_received {:natsbeam_msg, ^qsid, "jobs.c", _, _, _}
    assert received(collect(q1, ~r/never/, 300) <> collect(q2, ~r/never/, 300)) == 4

    # Go -> natsbeam queue member when Go has none: drop the Go members.
    Port.close(q1)
    Port.close(q2)
    Natsbeam.unsubscribe(qsid)
    {:ok, qsid2} = Natsbeam.subscribe("work.>", queue: "beamq")
    assert go_pub_until(go, qsid2, "work.1", "to-beam")
  end

  test "gateway messages are never relayed to other links", %{go: go} do
    # A plain natsbeam subscriber receives Go messages; a Go message must not
    # come back over our outbound connection (no echo, no relay).
    {:ok, sid} = Natsbeam.subscribe("loop.>")
    assert go_pub_until(go, sid, "loop.1", "once")
    sub = nats_bg(go.client, ["sub", "--count", "5", "loop.>"])
    collect(sub, ~r/Subscribing/)
    Process.sleep(500)
    before = status(:outbound).stats.out_msgs
    nats!(go.client, ["pub", "loop.2", "x"])
    assert_receive {:natsbeam_msg, ^sid, "loop.2", _, _, "x"}, 3_000
    Process.sleep(300)
    assert status(:outbound).stats.out_msgs == before
    assert received(collect(sub, ~r/never/, 300)) == 1
  end

  test "Go restart: both connections come back and traffic flows", %{go: go, port: port} do
    {:ok, sid} = Natsbeam.subscribe("re.>")
    assert go_pub_until(go, sid, "re.0", "before")

    GoServer.stop(go.os_pid)
    assert wait_until(fn -> not connected?(:outbound) and not connected?(:inbound) end, 10_000)

    conf = Path.join(go.dir, "server.conf")

    p =
      Port.open({:spawn_executable, GoServer.bin()}, [:binary, args: ["-c", conf, "-l", go.log]])

    {:os_pid, os_pid} = Port.info(p, :os_pid)
    on_exit(fn -> GoServer.stop(os_pid) end)
    assert wait_until(fn -> connected?(:outbound) and connected?(:inbound) end, 30_000)

    assert go_pub_until(go, sid, "re.1", "back")

    sub = nats_bg(go.client, ["sub", "--count", "1", "after.>"])
    collect(sub, ~r/Subscribing/)

    assert wait_until(
             fn ->
               nats(port, ["pub", "after.1", "restarted"])
               collect(sub, ~r/restarted/, 300) =~ "restarted"
             end,
             10_000
           )
  end

  @tag :optimistic
  test "optimistic mode: RS-/A- no-interest replies and the interest-only switch", %{
    go: go,
    port: port,
    gw_port: gw_port
  } do
    # Our system account heartbeats ($SYS.SERVER.<id>.STATSZ) would otherwise
    # count as outbound gateway traffic during the check.
    Natsbeam.set_system_account(nil)
    Process.sleep(300)
    inb = status(:inbound)
    refute inb.interest_only
    assert inb.modes["$G"] == nil

    # The system account is on by default; switch it off so natsbeam has no
    # interest in $SYS, then Go's system events (e.g. $SYS.ACCOUNT.*.CONNECT)
    # arrive optimistically for an account without interest: answered with A-.
    Natsbeam.set_system_account(nil)
    sys = Process.whereis(Natsbeam.SystemAccount)

    assert wait_until(fn ->
             Natsbeam.Sublist.subscriptions(elem(Natsbeam.Account.ensure("$SYS"), 2), sys) == []
           end)

    {:ok, sid} = Natsbeam.subscribe("opt.>")
    assert go_pub_until(go, sid, "opt.1", "optimistic")
    assert wait_until(fn -> status(:inbound).modes["$SYS"] == :no_interest end, 10_000)
    assert File.read!(go.log) =~ "A- $SYS"

    # Subjects without interest: RS- (once per subject)...
    nats!(go.client, ["pub", "none.1", "x"])
    assert wait_until(fn -> status(:inbound).stats.rs_minus >= 1 end, 5_000)
    assert wait_until(fn -> File.read!(go.log) =~ "RS- $G none.1" end, 5_000)

    # ...cleared by RS+ when a matching subscription appears.
    {:ok, sid2} = Natsbeam.subscribe("none.*")
    assert wait_until(fn -> File.read!(go.log) =~ "RS+ $G none.*" end, 5_000)
    assert go_pub_until(go, sid2, "none.1", "now")

    # More RS- than gateway_max_runsub (3): the account switches to interest-only.
    for i <- 2..6, do: nats!(go.client, ["pub", "other.#{i}", "x"])
    assert wait_until(fn -> status(:inbound).modes["$G"] == :interest_only end, 10_000)
    # Go's outbound (to our gateway port) processed INFO cmd 2 and cmd 3.
    complete =
      ~r/127\.0\.0\.1:#{gw_port} - gid:\d+ - Gateway "beam": switching account "\$G" to Interest-Only mode complete/

    assert wait_until(fn -> File.read!(go.log) =~ complete end, 5_000)

    # Interest-only from now on: new subscriptions are announced and work.
    {:ok, sid3} = Natsbeam.subscribe("late.>")
    assert go_pub_until(go, sid3, "late.1", "after-switch")
    assert go_pub_until(go, sid, "opt.2", "still")

    # natsbeam -> Go still fine.
    sub = nats_bg(go.client, ["sub", "--count", "1", "b.>"])
    collect(sub, ~r/Subscribing/)

    assert wait_until(
             fn ->
               nats(port, ["pub", "b.1", "hi"])
               collect(sub, ~r/hi/, 300) =~ "hi"
             end,
             10_000
           )
  end

  test "Go dials back an implicit gateway when not configured with our URL", %{gw_port: _} do
    # A second Go cluster that only knows... nothing about us: we dial it, it
    # learns our URL from INFO gateway_urls and connects back.
    go2 = GoServer.start!(jetstream: false, gateway: [name: "gocluster2", gateways: []])
    {:ok, _} = Natsbeam.add_gateway("nats://127.0.0.1:#{go2.gateway}", name: "gocluster2")

    on_exit(fn -> Natsbeam.remove_gateway("gocluster2") end)

    assert wait_until(
             fn ->
               roles =
                 for g <- Natsbeam.gateways(), g.gateway == "gocluster2", g.connected, do: g.role

               :outbound in roles and :inbound in roles
             end,
             15_000
           )

    {:ok, sid} = Natsbeam.subscribe("imp.>")
    assert go_pub_until(go2, sid, "imp.1", "implicit")
  end
end
