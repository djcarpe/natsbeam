defmodule Natsbeam.GoRouteTest do
  @moduledoc "natsbeam joined to a real Go nats-server cluster over the route protocol."
  use ExUnit.Case, async: false
  import Natsbeam.TestHelpers
  alias Natsbeam.GoServer
  @moduletag :go
  @moduletag timeout: 120_000

  # Go accounts for the pooling tests: "$SYS" (pinned), A (pool slot 0) and C (slot 1).
  @accounts """
  accounts {
    "$SYS": { users: [{user: sys, password: sys}] }
    A: { users: [{user: a, password: a}] }
    C: { users: [{user: c, password: c}] }
  }
  """

  # A Go system account under its own name: `system_account: SYS`.
  @sys_accounts """
  system_account: SYS
  accounts {
    SYS: { users: [{user: sys, password: sys}] }
    A: { users: [{user: a, password: a}] }
  }
  """

  # Tags: `beam: [config...]` for natsbeam, `accounts: true` for the Go
  # accounts above (`accounts: :sys` for the SYS ones), `go_dials: true` to
  # give Go a route to our cluster port.
  setup context do
    reset_config!()
    Natsbeam.Config.put(:cluster_name, "gocluster")
    for {k, v} <- context[:beam] || [], do: Natsbeam.Config.put(k, v)
    on_exit(&stop_links/0)

    http = GoServer.free_port()

    extra =
      "http: 127.0.0.1:#{http}\n" <>
        case context[:accounts] do
          :sys -> @sys_accounts
          true -> @accounts
          _ -> ""
        end

    # Go JetStream needs Raft peers once a cluster block exists, so routes are core-NATS only.
    go =
      if context[:go_dials] do
        rport = GoServer.free_port()
        {:ok, acceptor} = Natsbeam.start_route_listener(rport)

        on_exit(fn ->
          DynamicSupervisor.terminate_child(Natsbeam.AcceptorSupervisor, acceptor)
        end)

        cport = GoServer.free_port()

        cluster =
          "cluster { name: gocluster, listen: 127.0.0.1:#{cport}, routes: [nats-route://127.0.0.1:#{rport}] }\n"

        GoServer.start!(jetstream: false, extra: extra <> cluster) |> Map.put(:cluster, cport)
      else
        GoServer.start!(jetstream: false, cluster: true, extra: extra)
      end

    port = start_listener!()
    {:ok, go: Map.put(go, :http, http), port: port}
  end

  defp stop_links do
    for {_, pid, _, _} <- DynamicSupervisor.which_children(Natsbeam.LinkSupervisor),
        is_pid(pid),
        do: DynamicSupervisor.terminate_child(Natsbeam.LinkSupervisor, pid)
  end

  defp join!(go) do
    {:ok, pid} = Natsbeam.add_route("nats-route://127.0.0.1:#{go.cluster}")
    assert wait_until(fn -> Natsbeam.Link.status(pid).connected end, 10_000)
    pid
  end

  # Our connected route connections to `go`.
  defp route_links(go) do
    for %{kind: :route, connected: true, remote_name: name} = l <- Natsbeam.links(),
        name == go.name,
        do: l
  end

  defp routez(go, query \\ "") do
    {out, 0} = System.cmd("curl", ["-s", "http://127.0.0.1:#{go.http}/routez#{query}"])
    Jason.decode!(out)
  end

  defp beam_routes(go, query \\ "") do
    name = Natsbeam.Server.info().name
    for r <- routez(go, query)["routes"] || [], r["remote_name"] == name, do: r
  end

  defp received(port, timeout \\ 500) do
    collect(port, ~r/never/, timeout) |> String.split("Received") |> length() |> Kernel.-(1)
  end

  # Pub/sub both ways, request/reply from Go and a cross-server queue group,
  # all inside `account` (Go side authenticates as `user`).
  defp exercise_account!(go, account, user) do
    auth = if user, do: ["--user", user, "--password", user], else: []
    tag = String.downcase(account) |> String.replace(~r/\W/, "")

    {:ok, sid} = Natsbeam.subscribe("#{tag}.in.>", account: account)

    assert wait_until(
             fn ->
               nats(go.client, auth ++ ["pub", "#{tag}.in.1", "from go"])

               receive do
                 {:natsbeam_msg, ^sid, _, _, _, "from go"} -> true
               after
                 300 -> false
               end
             end,
             10_000
           ),
           "Go -> natsbeam in #{account}"

    sub = nats_bg(go.client, auth ++ ["sub", "--count", "1", "#{tag}.out.>"])
    collect(sub, ~r/Subscribing/)

    assert wait_until(
             fn ->
               Natsbeam.publish("#{tag}.out.1", "hello #{account}", account: account)
               collect(sub, ~r/hello/, 300) =~ "hello"
             end,
             10_000
           ),
           "natsbeam -> Go in #{account}"

    test_pid = self()

    responder =
      spawn_link(fn ->
        {:ok, s} = Natsbeam.subscribe("#{tag}.svc", account: account)
        send(test_pid, :responder_ready)

        Stream.repeatedly(fn ->
          receive do
            {:natsbeam_msg, ^s, _, reply, _, p} ->
              Natsbeam.publish(reply, "echo:" <> p, account: account)
          end
        end)
        |> Stream.run()
      end)

    assert_receive :responder_ready

    assert wait_until(
             fn ->
               {out, code} =
                 nats(go.client, auth ++ ["request", "#{tag}.svc", "ping", "--timeout", "1s"])

               code == 0 and out =~ "echo:ping"
             end,
             10_000
           ),
           "request/reply in #{account}"

    Process.unlink(responder)
    Process.exit(responder, :kill)

    q1 = nats_bg(go.client, auth ++ ["sub", "--queue", "q", "--count", "6", "#{tag}.q.>"])
    collect(q1, ~r/Subscribing/)
    {:ok, qsid} = Natsbeam.subscribe("#{tag}.q.>", queue: "q", account: account)
    Process.sleep(800)
    for i <- 1..6, do: Natsbeam.publish("#{tag}.q.x", "m#{i}", account: account)
    Process.sleep(800)

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

    assert local + received(q1) == 6, "queue group in #{account}"
    Natsbeam.unsubscribe(sid, account: account)
    Natsbeam.unsubscribe(qsid, account: account)
  end

  defp sid_account(%{"account" => acc}) when is_binary(acc), do: acc
  defp sid_account(%{"sid" => sid}), do: sid |> String.split(" ") |> hd()

  defp slot_link(go, slot), do: Enum.find(route_links(go), &(&1.slot == slot))

  @pool_slots [{:pool, 0}, {:pool, 1}, {:pool, 2}, {:account, "$SYS"}]

  defp assert_pool!(go) do
    assert wait_until(fn -> length(route_links(go)) == 4 end, 20_000),
           inspect(Natsbeam.links(), pretty: true)

    # stays that way: no duplicate connections flapping
    Process.sleep(2_000)
    links = route_links(go)
    assert Enum.sort(Enum.map(links, & &1.slot)) == Enum.sort(@pool_slots)
    assert Enum.all?(links, &(&1.pool_size == 3 or &1.route_account == "$SYS"))

    assert wait_until(fn -> length(beam_routes(go)) == 4 end, 5_000),
           inspect(routez(go), pretty: true)

    links
  end

  test "route handshake registers both servers and traffic flows both ways", %{go: go, port: port} do
    link = join!(go)
    st = Natsbeam.Link.status(link)
    assert st.remote_name == go.name and st.remote_cluster == "gocluster"

    # Go sees us as a route: its log mentions our server name
    assert wait_until(fn -> File.read!(go.log) =~ "Route connection created" end, 5_000)

    # natsbeam subscriber, Go publisher
    {:ok, sid} = Natsbeam.subscribe("r.>")

    assert wait_until(
             fn ->
               nats(go.client, ["pub", "r.1", "from go", "-H", "X-Go:1"])

               receive do
                 {:natsbeam_msg, ^sid, "r.1", _, hdrs, "from go"} -> hdrs =~ "X-Go: 1"
               after
                 300 -> false
               end
             end,
             10_000
           )

    # Go subscriber, natsbeam publisher
    sub = nats_bg(go.client, ["sub", "--count", "1", "s.>"])
    collect(sub, ~r/Subscribing/)

    assert wait_until(
             fn ->
               nats(port, ["pub", "s.1", "hello"]) |> elem(1) == 0 and
                 collect(sub, ~r/hello/, 300) =~ "hello"
             end,
             10_000
           )

    # request from Go answered on natsbeam
    spawn_link(fn ->
      {:ok, s} = Natsbeam.subscribe("svc.r")

      for _ <- 1..5 do
        receive do
          {:natsbeam_msg, ^s, _, reply, _, p} -> Natsbeam.publish(reply, "echo:" <> p)
        end
      end
    end)

    assert wait_until(
             fn ->
               match?({_, 0}, nats(go.client, ["request", "svc.r", "ping", "--timeout", "1s"]))
             end,
             10_000
           )

    # queue group across servers: one copy per message
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

  test "overlapping remote subscriptions get one copy per link", %{go: go, port: port} do
    join!(go)
    s1 = nats_bg(go.client, ["sub", "--count", "3", "ov.>"])
    s2 = nats_bg(go.client, ["sub", "--count", "3", "ov.a.*"])
    collect(s1, ~r/Subscribing/)
    collect(s2, ~r/Subscribing/)
    Process.sleep(1_500)
    nats!(port, ["pub", "ov.a.x", "once"])
    Process.sleep(800)
    out1 = collect(s1, ~r/never/, 300)
    out2 = collect(s2, ~r/never/, 300)
    assert length(String.split(out1, "Received")) - 1 == 1, out1
    assert length(String.split(out2, "Received")) - 1 == 1, out2
  end

  test "route survives a Go restart and interest is re-advertised", %{go: go, port: port} do
    link = join!(go)
    {:ok, sid} = Natsbeam.subscribe("re.>")

    assert wait_until(fn ->
             Enum.sum(Enum.map(route_links(go), & &1.interest_sent)) >= 1
           end)

    GoServer.stop(go.os_pid)
    assert wait_until(fn -> not Natsbeam.Link.status(link).connected end, 10_000)
    # restart Go on the same ports
    conf = Path.join(go.dir, "server.conf")

    restarted =
      Port.open({:spawn_executable, GoServer.bin()}, [:binary, args: ["-c", conf, "-l", go.log]])

    {:os_pid, os_pid} = Port.info(restarted, :os_pid)
    on_exit(fn -> GoServer.stop(os_pid) end)
    assert wait_until(fn -> Natsbeam.Link.status(link).connected end, 30_000)

    assert wait_until(
             fn ->
               nats(go.client, ["pub", "re.1", "back"])

               receive do
                 {:natsbeam_msg, ^sid, "re.1", _, _, "back"} -> true
               after
                 300 -> false
               end
             end,
             15_000
           )

    _ = port
  end

  # ----------------------------------------------------------- route pooling

  test "fnv32a and pool slots match nats-server" do
    # FNV-1a test vectors, and computeRoutePoolIdx(3, name)
    assert Natsbeam.Link.fnv32a("") == 0x811C9DC5
    assert Natsbeam.Link.fnv32a("a") == 0xE40C292C
    assert Natsbeam.Link.fnv32a("foobar") == 0xBF9CF968
    assert Natsbeam.Link.pool_slot("A", 3) == 0
    assert Natsbeam.Link.pool_slot("C", 3) == 1
    assert Natsbeam.Link.pool_slot("$G", 3) == 2
    assert Natsbeam.Link.pool_slot("anything", 1) == 0
  end

  @tag accounts: true
  test "we dial a pooled Go server: one connection per slot plus pinned $SYS, accounts on their fnv32a slot",
       %{go: go} do
    Natsbeam.set_system_account("$SYS")
    join!(go)
    links = assert_pool!(go)
    assert Enum.all?(links, &(&1.role == :solicited))

    # Go's view (/routez has no pool index, but the remote port of each route
    # is the local port of one of our connections): one route per link, the
    # $SYS one pinned.
    slot_by_port = Map.new(links, &{&1.local_port, &1.slot})
    routes = beam_routes(go)

    assert Enum.sort(Enum.map(routes, &slot_by_port[&1["port"]])) == Enum.sort(@pool_slots)

    assert Enum.all?(routes, fn r ->
             r["account"] == "$SYS" == (slot_by_port[r["port"]] == {:account, "$SYS"})
           end)

    # Interest of A (fnv32a slot 0) and C (slot 1) travels on different
    # connections, each the one its slot says.
    assert Natsbeam.Link.pool_slot("A", 3) == 0 and Natsbeam.Link.pool_slot("C", 3) == 1
    {:ok, _} = Natsbeam.subscribe("acct.a.only", account: "A")
    {:ok, _} = Natsbeam.subscribe("acct.c.only", account: "C")

    assert wait_until(
             fn ->
               subs =
                 for r <- beam_routes(go, "?subs=detail"),
                     s <- r["subscriptions_list_detail"] || [],
                     s["subject"] in ["acct.a.only", "acct.c.only"],
                     # a routed sub's sid is "<account> <subject>"
                     do: {sid_account(s), s["subject"], slot_by_port[r["port"]]}

               Enum.sort(subs) == [
                 {"A", "acct.a.only", {:pool, 0}},
                 {"C", "acct.c.only", {:pool, 1}}
               ]
             end,
             10_000
           ),
           inspect(beam_routes(go, "?subs=detail"), pretty: true)

    exercise_account!(go, "A", "a")
    exercise_account!(go, "C", "c")

    # inbound messages counted on the slot that carries each account
    assert slot_link(go, {:pool, 0}).stats.in_msgs > 0
    assert slot_link(go, {:pool, 1}).stats.in_msgs > 0
  end

  @tag accounts: true, go_dials: true
  test "a pooled Go server dialling us gets every pool slot and the pinned $SYS route accepted",
       %{go: go} do
    Natsbeam.set_system_account("$SYS")
    links = assert_pool!(go)
    assert Enum.all?(links, &(&1.role == :accepted))
    assert Enum.all?(beam_routes(go), &(&1["did_solicit"] == true))
    exercise_account!(go, "A", "a")
    exercise_account!(go, "C", "c")
  end

  @tag accounts: true, go_dials: true
  test "both servers dialling each other converge on one connection per slot", %{go: go} do
    Natsbeam.set_system_account("$SYS")
    # Our dial races Go's: each slot ends up on whichever connection the Go server kept.
    {:ok, _} = Natsbeam.add_route("nats-route://127.0.0.1:#{go.cluster}")
    assert_pool!(go)
    exercise_account!(go, "A", "a")
    exercise_account!(go, "C", "c")
  end

  @tag beam: [route_pool_size: 0]
  test "route_pool_size 0 keeps the legacy single no-pool connection", %{go: go} do
    link = join!(go)
    Process.sleep(1_500)
    assert [%{slot: :no_pool, pool_size: nil, pid: ^link}] = route_links(go)
    assert [route] = beam_routes(go)
    assert route["account"] in [nil, ""]
    exercise_account!(go, "$G", nil)
  end

  @tag accounts: true
  test "the pinned $SYS route carries $SYS traffic without account names", %{go: go} do
    Natsbeam.set_system_account("$SYS")
    join!(go)
    assert_pool!(go)
    pinned = slot_link(go, {:account, "$SYS"})
    assert pinned.route_account == "$SYS"

    # $SYS interest is advertised on the pinned connection only
    {:ok, sid} = Natsbeam.subscribe("$SYS.REQ.SERVER.PING", account: "$SYS")

    assert wait_until(fn ->
             subs =
               for r <- beam_routes(go, "?subs=detail"),
                   s <- r["subscriptions_list_detail"] || [],
                   s["subject"] == "$SYS.REQ.SERVER.PING",
                   do: r["account"]

             subs != [] and Enum.all?(subs, &(&1 == "$SYS"))
           end)

    for slot <- [0, 1, 2] do
      sent = :sys.get_state(slot_link(go, {:pool, slot}).pid).sent
      refute Enum.any?(Map.keys(sent), &match?({"$SYS", _, _}, &1)), "slot #{slot} carries $SYS"
    end

    # A request from the Go side reaches our $SYS responder over the pinned route.
    assert wait_until(
             fn ->
               nats(go.client, [
                 "--user",
                 "sys",
                 "--password",
                 "sys",
                 "request",
                 "$SYS.REQ.SERVER.PING",
                 "",
                 "--replies",
                 "0",
                 "--timeout",
                 "500ms"
               ])

               receive do
                 {:natsbeam_msg, ^sid, "$SYS.REQ.SERVER.PING", reply, _, _} -> is_binary(reply)
               after
                 300 -> false
               end
             end,
             10_000
           )

    assert slot_link(go, {:account, "$SYS"}).stats.in_msgs > 0
  end

  # ------------------------------------------------------------ system account

  defp sys_nats(port, args), do: nats(port, ["--user", "sys", "--password", "sys" | args])

  defp beam_name, do: Natsbeam.Server.info().name

  # Configure our side like the Go one: system account "SYS" with a sys user.
  defp beam_sys! do
    Natsbeam.set_system_account("SYS")
    Natsbeam.add_user("sys", password: "sys", account: "SYS")
  end

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

  @tag accounts: :sys
  test "Go `nats server list/ping/info/report` with system_account SYS include natsbeam, and back",
       %{go: go, port: port} do
    beam_sys!()
    join!(go)

    # the system account gets Go's pinned route under its configured name
    assert wait_until(fn -> slot_link(go, {:account, "SYS"}) != nil end, 20_000),
           inspect(Natsbeam.links(), pretty: true)

    assert wait_until(fn -> Enum.any?(beam_routes(go), &(&1["account"] == "SYS")) end, 10_000)

    # Go side: list, ping, info, report connections, report jetstream
    assert_server_list!(go.client, [go.name, beam_name()])

    out = sys_nats(go.client, ["server", "ping", "2"]) |> elem(0)
    assert out =~ beam_name() and out =~ go.name, out

    {info, 0} = sys_nats(go.client, ["server", "info", beam_name()])
    assert info =~ Natsbeam.Server.info().id, info
    assert info =~ "Process Details", info

    # a client on our side shows up in the connection report
    bg = nats_bg(port, ["--user", "sys", "--password", "sys", "sub", "report.conns"])
    collect(bg, ~r/Subscribing/)
    {conns, 0} = sys_nats(go.client, ["server", "report", "connections", "2"])
    assert conns =~ go.name and conns =~ beam_name(), conns

    {js, 0} = sys_nats(go.client, ["server", "report", "jetstream", "2"])
    assert js =~ go.name and js =~ beam_name(), js

    # natsbeam side: the same tooling lists the Go server through the link
    assert_server_list!(port, [go.name, beam_name()])
    {info, 0} = sys_nats(port, ["server", "info", go.name])
    assert info =~ go.name, info

    # Go tracks us from our STATSZ heartbeats; we track Go from its
    assert wait_until(
             fn ->
               Enum.any?(Natsbeam.SystemAccount.remote_servers(), fn {_, si} ->
                 si["name"] == go.name
               end)
             end,
             15_000
           )

    # filters: a ping aimed at the Go server by name is not answered by us
    {out, 0} =
      sys_nats(go.client, [
        "request",
        "$SYS.REQ.SERVER.PING.IDZ",
        ~s({"server_name": "#{go.name}", "exact_match": true}),
        "--replies",
        "0",
        "--timeout",
        "1s"
      ])

    refute out =~ Natsbeam.Server.info().id, out

    # our STATSZ heartbeats are in order and well formed as far as Go is concerned
    log = File.read!(go.log)
    refute log =~ "out of order remote server update"
    refute log =~ "bad server info"
    refute log =~ ~r/\[ERR\].*(\$SYS|STATSZ|SYS")/
  end

  @tag accounts: :sys
  test "Go learns of our shutdown from $SYS.SERVER.<id>.SHUTDOWN", %{go: go} do
    beam_sys!()
    Natsbeam.Config.put(:statsz_interval, 1_000)
    join!(go)
    assert wait_until(fn -> slot_link(go, {:account, "SYS"}) != nil end, 20_000)
    id = Natsbeam.Server.info().id

    {:ok, sid} = Natsbeam.subscribe("$SYS.SERVER.#{id}.>", account: "SYS")
    mon = nats_bg(go.client, ["--user", "sys", "--password", "sys", "sub", "$SYS.SERVER.#{id}.>"])
    collect(mon, ~r/Subscribing/)
    assert collect(mon, ~r/STATSZ/, 10_000) =~ "STATSZ"

    Natsbeam.SystemAccount.announce_shutdown()
    assert collect(mon, ~r/SHUTDOWN/, 5_000) =~ "SHUTDOWN"
    assert_receive {:natsbeam_msg, ^sid, "$SYS.SERVER." <> _, _, _, _}, 2_000
  end
end
