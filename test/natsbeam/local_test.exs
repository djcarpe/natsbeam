defmodule Natsbeam.LocalTest do
  use ExUnit.Case, async: true

  test "subscribe/publish within the VM" do
    {:ok, sid} = Natsbeam.subscribe("loc.*", account: "LOCAL")
    {:ok, 1} = Natsbeam.publish("loc.a", "x", account: "LOCAL")
    assert_receive {:natsbeam_msg, ^sid, "loc.a", nil, nil, "x"}
    {:ok, 0} = Natsbeam.publish("loc.a", "x", account: "OTHER")
    :ok = Natsbeam.unsubscribe(sid, account: "LOCAL")
    {:ok, 0} = Natsbeam.publish("loc.a", "x", account: "LOCAL")
  end

  test "request/reply within the VM" do
    parent = self()

    responder =
      spawn_link(fn ->
        {:ok, _} = Natsbeam.subscribe("svc.add", account: "LOCAL")
        send(parent, :ready)

        receive do
          {:natsbeam_msg, _, _, reply, _, payload} ->
            Natsbeam.publish(reply, "#{String.to_integer(payload) + 1}", account: "LOCAL")
        end
      end)

    assert_receive :ready
    assert {:ok, %{payload: "42"}} = Natsbeam.request("svc.add", "41", account: "LOCAL")
    assert {:error, :no_responders} = Natsbeam.request("svc.none", "41", account: "LOCAL")
    _ = responder
  end

  test "subscriptions die with their process" do
    {:ok, _apid, sublist} = Natsbeam.Account.ensure("DYING")

    pid =
      spawn(fn ->
        Natsbeam.subscribe("x", account: "DYING")
        receive do: (:stop -> :ok)
      end)

    assert Natsbeam.TestHelpers.wait_until(fn -> Natsbeam.Sublist.count(sublist) == 1 end)
    send(pid, :stop)
    assert Natsbeam.TestHelpers.wait_until(fn -> Natsbeam.Sublist.count(sublist) == 0 end)
  end
end
