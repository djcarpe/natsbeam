defmodule Natsbeam.CLITest do
  @moduledoc "Interop with the official `nats` CLI (skipped when it isn't installed)."
  use ExUnit.Case, async: false
  import Natsbeam.TestHelpers
  @moduletag :cli

  setup do
    reset_config!()
    {:ok, port: start_listener!()}
  end

  test "pub/sub with headers", %{port: port} do
    sub = nats_bg(port, ["sub", "--count", "1", "cli.>"])
    collect(sub, ~r/Subscribing/)
    nats!(port, ["pub", "cli.hello", "payload", "-H", "X-Test:1"])
    out = collect(sub, ~r/payload/)
    assert out =~ ~s(Received on "cli.hello")
    assert out =~ "X-Test: 1"
  end

  test "request/reply and no responders", %{port: port} do
    {out, _code} = nats(port, ["request", "nobody", "x", "--timeout", "500ms"])
    assert out =~ "No responders"

    rep = nats_bg(port, ["reply", "svc.echo", "pong:{{Body}}", "--count", "1"])
    collect(rep, ~r/Listening/)
    assert nats!(port, ["request", "svc.echo", "ping"]) =~ "pong:"
  end

  test "username/password and token", %{port: port} do
    Natsbeam.add_user("alice", password: "pw", account: "A")
    assert nats!(port, ["pub", "--user", "alice", "--password", "pw", "x", "y"]) =~ "Published"
    {out, code} = nats(port, ["pub", "--user", "alice", "--password", "no", "x", "y"])
    assert code != 0 and out =~ "Authorization Violation"

    Natsbeam.set_token("tok")
    Natsbeam.remove_user("alice")
    assert nats!(port, ["pub", "--token", "tok", "x", "y"]) =~ "Published"
  end

  test "nkey file", %{port: port} do
    {seed, pub} = Natsbeam.NKeys.generate(:user)
    path = Path.join(System.tmp_dir!(), "natsbeam-#{System.unique_integer([:positive])}.nk")
    File.write!(path, seed <> "\n")
    on_exit(fn -> File.rm(path) end)
    Natsbeam.add_nkey(pub, account: "NK")
    assert nats!(port, ["pub", "--nkey", path, "x", "y"]) =~ "Published"
  end

  test "creds file in operator mode with permissions", %{port: port} do
    f =
      operator_fixture(
        user: %{
          "pub" => %{"allow" => ["ok.>", "_INBOX.>"]},
          "sub" => %{"allow" => ["ok.>", "_INBOX.>"]}
        }
      )

    {:ok, _} = Natsbeam.add_operator(f.op_jwt)
    {:ok, _} = Natsbeam.add_account_jwt(f.acc_jwt)
    creds = creds_file(f.usr_jwt, f.usr_seed)

    sub = nats_bg(port, ["sub", "--creds", creds, "--count", "1", "ok.>"])
    collect(sub, ~r/Subscribing/)
    assert nats!(port, ["pub", "--creds", creds, "ok.one", "hello"]) =~ "Published"
    assert collect(sub, ~r/hello/) =~ "hello"

    {out, code} = nats(port, ["pub", "--creds", creds, "denied", "hello"])
    assert code != 0 and out =~ ~s(Permissions Violation for Publish to "denied")

    {out, code} = nats(port, ["pub", "denied", "hello"])
    assert code != 0 and out =~ "Authorization Violation"
  end

  test "in-VM subscriber talks to CLI publisher and vice versa", %{port: port} do
    {:ok, _sid} = Natsbeam.subscribe("vm.>")
    nats!(port, ["pub", "vm.hi", "from cli"])
    assert_receive {:natsbeam_msg, _, "vm.hi", nil, nil, "from cli"}, 2_000

    sub = nats_bg(port, ["sub", "--count", "1", "cli.>"])
    collect(sub, ~r/Subscribing/)
    # "Subscribing" is printed before the SUB reaches the server; wait for interest.
    {:ok, _, sublist} = Natsbeam.Account.lookup("$G")
    assert wait_until(fn -> Natsbeam.Sublist.interest?(sublist, "cli.x") end)
    {:ok, 1} = Natsbeam.publish("cli.x", "from vm", headers: [{"X-From", "beam"}])
    out = collect(sub, ~r/from vm/)
    assert out =~ "X-From: beam"
  end
end
