defmodule Natsbeam.SystemAccountTest do
  use ExUnit.Case, async: false
  import Natsbeam.TestHelpers

  setup do
    reset_config!()
    :ok
  end

  test "claims can be pushed and looked up over $SYS in operator mode" do
    f = operator_fixture()
    {sys_seed, sys_pub} = Natsbeam.NKeys.generate(:account)

    {:ok, sys_jwt} =
      Natsbeam.JWT.sign(
        %{"sub" => sys_pub, "name" => "SYS", "nats" => %{"type" => "account"}},
        f.op_seed
      )

    {:ok, op_jwt} =
      Natsbeam.JWT.sign(
        %{"sub" => f.op_pub, "nats" => %{"type" => "operator", "system_account" => sys_pub}},
        f.op_seed
      )

    _ = sys_seed

    {:ok, _} = Natsbeam.add_operator(op_jwt)
    {:ok, _} = Natsbeam.add_account_jwt(sys_jwt)
    assert Natsbeam.Config.get([:auth, :system_account]) == sys_pub

    assert wait_until(fn ->
             Natsbeam.Sublist.count(elem(Natsbeam.Account.ensure(sys_pub), 2)) > 0
           end)

    # push the ACME account the way `nsc push` does
    {:ok, %{payload: body}} =
      Natsbeam.request("$SYS.REQ.CLAIMS.UPDATE", f.acc_jwt, account: sys_pub)

    assert %{"data" => %{"code" => 200, "account" => acc}} = Jason.decode!(body)
    assert acc == f.acc_pub
    assert f.acc_pub in Natsbeam.account_jwts()

    {:ok, %{payload: raw}} =
      Natsbeam.request("$SYS.REQ.ACCOUNT.#{f.acc_pub}.CLAIMS.LOOKUP", "", account: sys_pub)

    assert raw == f.acc_jwt

    {:ok, %{payload: list}} = Natsbeam.request("$SYS.REQ.CLAIMS.LIST", "", account: sys_pub)
    assert f.acc_pub in Jason.decode!(list)

    # a JWT from a foreign operator is rejected
    other = operator_fixture()

    {:ok, %{payload: body}} =
      Natsbeam.request("$SYS.REQ.CLAIMS.UPDATE", other.acc_jwt, account: sys_pub)

    assert %{"error" => %{"code" => 400}} = Jason.decode!(body)

    # server ping / varz answer with server info
    {:ok, %{payload: ping}} = Natsbeam.request("$SYS.REQ.SERVER.PING", "", account: sys_pub)

    assert %{"server" => %{"id" => "N" <> _}, "statsz" => %{"connections" => _}} =
             Jason.decode!(ping)

    srv_id = Natsbeam.Server.info().id

    {:ok, %{payload: varz}} =
      Natsbeam.request("$SYS.REQ.SERVER.#{srv_id}.VARZ", "", account: sys_pub)

    assert %{"data" => %{"server_id" => ^srv_id, "routes" => 0}} = Jason.decode!(varz)

    # delete via operator-signed generic claims
    {:ok, del} =
      Natsbeam.JWT.sign(
        %{"sub" => f.op_pub, "nats" => %{"type" => "generic", "accounts" => [f.acc_pub]}},
        f.op_seed
      )

    {:ok, %{payload: body}} = Natsbeam.request("$SYS.REQ.CLAIMS.DELETE", del, account: sys_pub)
    assert %{"data" => %{"code" => 200}} = Jason.decode!(body)
    refute f.acc_pub in Natsbeam.account_jwts()
  end

  test "system account can be set in static mode" do
    Natsbeam.set_system_account("SYS")

    assert wait_until(fn ->
             Natsbeam.Sublist.interest?(
               elem(Natsbeam.Account.ensure("SYS"), 2),
               "$SYS.REQ.SERVER.PING"
             )
           end)

    {:ok, %{payload: ping}} = Natsbeam.request("$SYS.REQ.SERVER.PING", "", account: "SYS")
    assert %{"statsz" => _} = Jason.decode!(ping)
    Natsbeam.set_system_account(nil)

    assert wait_until(fn ->
             Natsbeam.Sublist.count(elem(Natsbeam.Account.ensure("SYS"), 2)) == 0
           end)
  end

  test "the system account defaults to $SYS, like nats-server" do
    assert Natsbeam.Config.get([:auth, :system_account]) == "$SYS"
    assert "$SYS" in Natsbeam.Link.pinned_accounts()
  end

  defp sys_ready!(account) do
    assert wait_until(fn ->
             match?(
               {:ok, _},
               Natsbeam.request("$SYS.REQ.SERVER.PING.IDZ", "", account: account, timeout: 200)
             )
           end)
  end

  # All replies to one request within `ms`.
  defp replies(subject, body, account, ms \\ 300) do
    inbox = "_INBOX.t#{System.unique_integer([:positive])}"
    {:ok, sid} = Natsbeam.subscribe(inbox, account: account)
    Natsbeam.publish(subject, body, account: account, reply: inbox)
    Process.sleep(ms)

    msgs =
      Stream.repeatedly(fn ->
        receive do
          {:natsbeam_msg, ^sid, _, _, _, p} -> Jason.decode!(p)
        after
          0 -> nil
        end
      end)
      |> Enum.take_while(& &1)

    Natsbeam.unsubscribe(sid, account: account)
    msgs
  end

  test "server requests: one reply each, Go-shaped server info, filters honoured" do
    Natsbeam.set_system_account("SYS")
    sys_ready!("SYS")
    srv = Natsbeam.Server.info()

    for z <- ~w(STATSZ VARZ CONNZ ROUTEZ LEAFZ SUBSZ GATEWAYZ HEALTHZ ACCOUNTZ JSZ) do
      assert [reply] = replies("$SYS.REQ.SERVER.PING.#{z}", "", "SYS"), z
      assert [_] = replies("$SYS.REQ.SERVER.#{srv.id}.#{z}", "", "SYS"), z

      assert %{"server" => %{"id" => id, "name" => _, "ver" => _, "seq" => seq, "time" => _}} =
               reply

      assert id == srv.id and seq > 0
      assert Map.has_key?(reply, if(z == "STATSZ", do: "statsz", else: "data")), z
    end

    assert [%{"server" => %{"cluster" => _}, "statsz" => %{"active_servers" => 1}}] =
             replies("$SYS.REQ.SERVER.PING", "", "SYS")

    assert [%{"id" => id}] = replies("$SYS.REQ.SERVER.PING.IDZ", "", "SYS")
    assert id == srv.id

    # requests addressed to another server id are not ours
    assert [] = replies("$SYS.REQ.SERVER.NOTME.VARZ", "", "SYS")

    # EventFilterOptions: contains-match by default, exact on request
    assert [_] = replies("$SYS.REQ.SERVER.PING.VARZ", ~s({"server_name":"#{srv.name}"}), "SYS")

    assert [] =
             replies(
               "$SYS.REQ.SERVER.PING.VARZ",
               ~s({"server_name":"#{String.slice(srv.name, 0, 3)}","exact_match":true}),
               "SYS"
             )

    assert [] = replies("$SYS.REQ.SERVER.PING", ~s({"cluster":"elsewhere"}), "SYS")
    assert [] = replies("$SYS.REQ.SERVER.PING.JSZ", ~s({"domain":"nope"}), "SYS")
    assert [] = replies("$SYS.REQ.SERVER.PING.STATSZ", ~s({"tags":["gpu"]}), "SYS")

    assert [%{"error" => %{"code" => 400}}] =
             replies("$SYS.REQ.SERVER.PING.VARZ", "{not json", "SYS")
  end

  test "STATSZ heartbeats with growing seq, on demand by a reply-less PING" do
    Natsbeam.Config.put(:statsz_interval, 300)
    Natsbeam.set_system_account("SYS")
    sys_ready!("SYS")
    id = Natsbeam.Server.info().id
    {:ok, sid} = Natsbeam.subscribe("$SYS.SERVER.#{id}.STATSZ", account: "SYS")

    seqs =
      for _ <- 1..3 do
        assert_receive {:natsbeam_msg, ^sid, _, nil, _, p}, 2_000
        %{"server" => %{"seq" => seq}, "statsz" => %{"connections" => _}} = Jason.decode!(p)
        seq
      end

    assert seqs == Enum.sort(seqs) and length(Enum.uniq(seqs)) == 3

    Natsbeam.Config.put(:statsz_interval, 60_000)
    # drain, then ask for one without a reply subject
    Process.sleep(700)
    flush(sid)
    Natsbeam.publish("$SYS.REQ.SERVER.PING", "", account: "SYS")
    assert_receive {:natsbeam_msg, ^sid, _, nil, _, _}, 1_000

    # other servers' heartbeats are tracked until they say goodbye
    other = %{"server" => %{"id" => "NOTHER", "name" => "other", "seq" => 1}, "statsz" => %{}}
    Natsbeam.publish("$SYS.SERVER.NOTHER.STATSZ", Jason.encode!(other), account: "SYS")
    assert wait_until(fn -> Map.has_key?(Natsbeam.SystemAccount.remote_servers(), "NOTHER") end)

    assert [%{"statsz" => %{"active_servers" => 2}}] =
             replies("$SYS.REQ.SERVER.PING", "", "SYS")

    Natsbeam.publish("$SYS.SERVER.NOTHER.SHUTDOWN", ~s({"id":"NOTHER"}), account: "SYS")
    assert wait_until(fn -> Natsbeam.SystemAccount.remote_servers() == %{} end)

    # our own goodbye
    {:ok, bye} = Natsbeam.subscribe("$SYS.SERVER.#{id}.SHUTDOWN", account: "SYS")
    :ok = Natsbeam.SystemAccount.announce_shutdown()
    assert_receive {:natsbeam_msg, ^bye, _, _, _, p}, 1_000
    assert %{"id" => ^id} = Jason.decode!(p)
  end

  defp flush(sid) do
    receive do
      {:natsbeam_msg, ^sid, _, _, _, _} -> flush(sid)
    after
      0 -> :ok
    end
  end
end
