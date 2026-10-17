defmodule Natsbeam.JetStreamAPILifecycleTest do
  @moduledoc """
  The per-account `$JS.API.>` responder follows config changes in place: a
  JetStream-enabled account always has exactly one responder, with no gap
  while the configuration flips around it.
  """
  use ExUnit.Case, async: false
  import Natsbeam.TestHelpers

  @acc "JSAPILIFE"

  setup do
    reset_config!()
    on_exit(fn -> Natsbeam.remove_account(@acc) end)
    :ok
  end

  defp responders(account) do
    {:ok, _, sublist} = Natsbeam.Account.ensure(account)
    Natsbeam.Sublist.weight(sublist, "$JS.API.>", "_natsbeam_jsapi")
  end

  defp info(account), do: Natsbeam.request("$JS.API.INFO", "", account: account, timeout: 2_000)

  # Regression: reset_config! (JetStream off for the account) followed by
  # add_account(jetstream: true) raced the Manager's asynchronous stop of the
  # responder, so the next request found no responder.
  test "toggling JetStream config under load never leaves an enabled account without a responder" do
    hogs =
      for _ <- 1..(System.schedulers_online() * 2) do
        spawn(fn ->
          Stream.repeatedly(fn -> :erlang.phash2(:rand.uniform()) end) |> Stream.run()
        end)
      end

    on_exit(fn -> Enum.each(hogs, &Process.exit(&1, :kill)) end)

    failures =
      for _ <- 1..400, reduce: [] do
        acc ->
          reset_config!()
          {:ok, _} = Natsbeam.add_account(@acc, jetstream: true)

          case info(@acc) do
            {:ok, _} -> acc
            other -> [other | acc]
          end
      end

    assert failures == []
    assert responders(@acc) == 1
  end

  test "disabling withdraws the responder in place and re-enabling restores it" do
    {:ok, _} = Natsbeam.add_account(@acc, jetstream: true)
    [{pid, _}] = Registry.lookup(Natsbeam.Registry, {:js_api, @acc})
    assert {:ok, _} = info(@acc)
    assert responders(@acc) == 1

    {:ok, _} = Natsbeam.add_account(@acc, jetstream: false)
    assert {:error, :no_responders} = info(@acc)
    assert responders(@acc) == 0

    {:ok, _} = Natsbeam.add_account(@acc, jetstream: true)
    assert {:ok, _} = info(@acc)
    assert responders(@acc) == 1
    # same process throughout: updated in place, never restarted
    assert [{^pid, _}] = Registry.lookup(Natsbeam.Registry, {:js_api, @acc})
  end

  test "a restarted account gets exactly one responder again" do
    {:ok, _} = Natsbeam.add_account(@acc, jetstream: true)
    assert {:ok, _} = info(@acc)
    Natsbeam.remove_account(@acc)
    {:ok, _} = Natsbeam.add_account(@acc, jetstream: true)
    assert {:ok, _} = info(@acc)
    assert responders(@acc) == 1
  end
end
