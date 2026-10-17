defmodule Natsbeam.Application do
  @moduledoc false
  use Application

  @impl true
  def start(_type, _args) do
    children = [
      {Registry, keys: :unique, name: Natsbeam.Registry},
      {Registry, keys: :unique, name: Natsbeam.ClientRegistry},
      %{id: :pg, start: {:pg, :start_link, [Natsbeam.PG]}},
      Natsbeam.Config,
      Natsbeam.Server,
      {DynamicSupervisor, name: Natsbeam.AccountSupervisor, strategy: :one_for_one},
      {DynamicSupervisor, name: Natsbeam.ListenerSupervisor, strategy: :one_for_one},
      Natsbeam.SystemAccount,
      Natsbeam.JetStream,
      {DynamicSupervisor, name: Natsbeam.LinkSupervisor, strategy: :one_for_one},
      {DynamicSupervisor, name: Natsbeam.AcceptorSupervisor, strategy: :one_for_one},
      Natsbeam.Boot
    ]

    Supervisor.start_link(children, strategy: :rest_for_one, name: Natsbeam.Supervisor)
  end

  # Tell other servers we are going (`$SYS.SERVER.<id>.SHUTDOWN`) while the
  # links are still up; the tree stops links before the system account.
  @impl true
  def prep_stop(state) do
    Natsbeam.SystemAccount.announce_shutdown()
    state
  end
end
