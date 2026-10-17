defmodule Natsbeam.DynamicTest do
  use ExUnit.Case, async: false
  import Natsbeam.TestHelpers

  setup do
    reset_config!()
    :ok
  end

  test "listeners start and stop at runtime" do
    {:ok, _} = Natsbeam.start_listener(name: :dyn, port: 0, host: "127.0.0.1")
    assert %{dyn: port} = Natsbeam.listeners()
    {sock, _} = connect!(port)
    :ok = Natsbeam.stop_listener(:dyn)
    assert "-ERR 'Server Shutdown'" = recv(sock, 1_000)
    assert {:error, :closed} = recv(sock, 1_000)
    refute Map.has_key?(Natsbeam.listeners(), :dyn)
    assert {:error, :not_found} = Natsbeam.stop_listener(:dyn)
  end

  test "accounts can be added, inspected and removed" do
    port = start_listener!()
    {:ok, _} = Natsbeam.add_account("TENANT", max_connections: 1)
    Natsbeam.add_user("t", password: "t", account: "TENANT")
    {a, _} = connect!(port, %{"user" => "t", "pass" => "t"})
    {b, _} = connect(port)
    send!(b, ~s(CONNECT {"user":"t","pass":"t"}\r\n))
    assert "-ERR 'maximum account active connections exceeded'" = recv(b)
    assert %{name: "TENANT", clients: 1} = Enum.find(Natsbeam.accounts(), &(&1.name == "TENANT"))
    :ok = Natsbeam.remove_account("TENANT")
    assert "-ERR 'Account Removed'" = recv(a)
    assert wait_until(fn -> "TENANT" not in Natsbeam.Account.list() end)
  end

  test "config changes emit telemetry" do
    :telemetry.attach(
      "cfg-test",
      [:natsbeam, :config, :changed],
      fn _, _, meta, pid -> send(pid, {:changed, meta}) end,
      self()
    )

    Natsbeam.Config.put(:max_payload, 123)
    assert_receive {:changed, %{new: %{max_payload: 123}}}
    :telemetry.detach("cfg-test")
  end
end
