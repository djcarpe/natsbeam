defmodule Natsbeam.Config do
  @moduledoc """
  Live server configuration.

  The configuration is an ordinary map held in an ETS table so every connection
  reads it lock-free, and mutated at runtime through `update/1`. Changes are
  broadcast as `[:natsbeam, :config, :changed]` telemetry events.

  Initial values come from `Application.get_env(:natsbeam, ...)`; see
  `defaults/0` for the full set of keys.
  """
  use GenServer

  @table __MODULE__

  @type t :: map()

  def defaults do
    %{
      server_name: nil,
      listeners: [%{name: :default, port: 4222, host: "0.0.0.0"}],
      max_payload: 1_048_576,
      max_control_line: 4096,
      max_connections: 65_536,
      max_subscriptions: 0,
      max_pending_msgs: 65_536,
      ping_interval: 120_000,
      ping_max_outstanding: 2,
      auth_timeout: 2_000,
      write_timeout: 10_000,
      advertised_version: "2.11.0",
      # Server-to-server links with nats-server (routes and leafnodes).
      cluster_name: nil,
      cluster_port: nil,
      cluster_auth: nil,
      routes: [],
      # Route pooling (nats-server `cluster.pool_size`): connections per peer
      # that accounts are spread over by fnv32a(name) % size. 0 or -1 = one
      # connection carrying every account (no-pool mode).
      route_pool_size: 3,
      # Accounts with a dedicated route connection (nats-server
      # `cluster.accounts`); the system account is always added.
      pinned_accounts: [],
      leaf_port: nil,
      leaf_remotes: [],
      lame_duck_grace: 2_000,
      # System account events: `$SYS.SERVER.<id>.STATSZ` period (nats-server's
      # 10s), and the tags `$SYS.REQ.SERVER.*` filters match against.
      statsz_interval: 10_000,
      server_tags: [],
      # Gateways (super-cluster). Our gateway name is the cluster name.
      # gateways: [%{name: "remote", url: "nats://host:7222"}]
      gateway_port: nil,
      gateway_host: nil,
      gateway_advertise: nil,
      gateways: [],
      gateway_auth: nil,
      gateway_reject_unknown: false,
      # Switch every account to interest-only mode on accepting a gateway
      # (nats-server >= 2.9 behaviour); false = optimistic with A-/RS- replies.
      gateway_interest_only: true,
      # RS- sent for an account before switching it to interest-only.
      gateway_max_runsub: 1_000,
      auth: %{
        # Shared-token auth. May be a bcrypt hash.
        token: nil,
        # name => %{password:, account:, permissions:}
        users: %{},
        # public_key => %{account:, permissions:, user:}
        nkeys: %{},
        # Operator mode: trusted operator JWT claims (verified) by public key.
        operators: %{},
        # account public key => verified account claims (memory resolver).
        account_jwts: %{},
        # account public key => raw JWT string (served to $SYS lookups).
        account_jwt_raw: %{},
        # Optional custom provider: {module, opts} implementing Natsbeam.Auth.Provider.
        provider: nil,
        # Name (static mode) or public key (operator mode) of the system
        # account; nil disables it. "$SYS" like nats-server's default; set it
        # to the Go side's `system_account` name (e.g. "SYS") when linking.
        system_account: "$SYS",
        # Default account for identities that don't name one.
        default_account: "$G"
      },
      # name => %{max_connections:, max_subscriptions:, max_payload:, jetstream:}
      accounts: %{},
      cluster: true,
      jetstream: %{enabled: true, store_dir: nil, max_memory: -1, max_storage: -1, domain: nil}
    }
  end

  def start_link(opts) do
    GenServer.start_link(__MODULE__, opts, name: __MODULE__)
  end

  @impl true
  def init(_opts) do
    :ets.new(@table, [:named_table, :set, :public, read_concurrency: true])
    initial = from_application_env()
    :ets.insert(@table, {:config, initial})
    {:ok, %{}}
  end

  @doc "The whole live configuration."
  @spec all() :: t()
  def all do
    [{:config, cfg}] = :ets.lookup(@table, :config)
    cfg
  end

  @doc "Read one top-level key, or a path of keys."
  @spec get(atom() | [atom()], term()) :: term()
  def get(key_or_path, default \\ nil)
  def get(path, default) when is_list(path), do: get_in(all(), path) || default
  def get(key, default), do: Map.get(all(), key, default)

  @doc "Replace one top-level key."
  def put(key, value), do: update(&Map.put(&1, key, value))

  @doc """
  Apply `fun` to the config atomically and publish the result. `fun` receives
  the current config and must return the new one.
  """
  @spec update((t() -> t())) :: t()
  def update(fun) when is_function(fun, 1) do
    GenServer.call(__MODULE__, {:update, fun})
  end

  @doc "Update a nested path with `fun`."
  def update_in(path, fun), do: update(&Kernel.update_in(&1, path, fun))

  @doc "Put a value at a nested path."
  def put_in(path, value), do: update(&Kernel.put_in(&1, path, value))

  @impl true
  def handle_call({:update, fun}, _from, state) do
    old = all()
    new = fun.(old)
    :ets.insert(@table, {:config, new})
    :telemetry.execute([:natsbeam, :config, :changed], %{}, %{old: old, new: new})
    {:reply, new, state}
  end

  @doc false
  def from_application_env do
    env = Application.get_all_env(:natsbeam)
    cfg = defaults()

    Enum.reduce(env, cfg, fn
      {:auth, auth}, acc when is_list(auth) or is_map(auth) ->
        Map.update!(acc, :auth, &deep_merge(&1, normalize_auth(Map.new(auth))))

      {:listeners, listeners}, acc when is_list(listeners) ->
        Map.put(acc, :listeners, Enum.map(listeners, &Map.new/1))

      {:accounts, accounts}, acc ->
        Map.put(acc, :accounts, Map.new(accounts, fn {k, v} -> {to_string(k), Map.new(v)} end))

      {:jetstream, js}, acc when is_list(js) or is_map(js) ->
        Map.update!(acc, :jetstream, &Map.merge(&1, Map.new(js)))

      {k, v}, acc when is_map_key(acc, k) ->
        Map.put(acc, k, v)

      _, acc ->
        acc
    end)
  end

  @doc false
  def normalize_auth(auth) do
    auth
    |> Map.update(:users, %{}, &normalize_users/1)
    |> Map.update(:nkeys, %{}, &normalize_nkeys/1)
  end

  defp normalize_users(users) when is_list(users) do
    Map.new(users, fn u ->
      u = Map.new(u)
      name = Map.fetch!(u, :user)
      {name, Natsbeam.Auth.normalize_user(Map.delete(u, :user))}
    end)
  end

  defp normalize_users(users) when is_map(users),
    do: Map.new(users, fn {k, v} -> {to_string(k), Natsbeam.Auth.normalize_user(Map.new(v))} end)

  defp normalize_nkeys(nkeys) when is_list(nkeys) do
    Map.new(nkeys, fn n ->
      n = Map.new(n)
      key = Map.fetch!(n, :nkey)
      {key, Natsbeam.Auth.normalize_user(Map.delete(n, :nkey))}
    end)
  end

  defp normalize_nkeys(nkeys) when is_map(nkeys),
    do: Map.new(nkeys, fn {k, v} -> {k, Natsbeam.Auth.normalize_user(Map.new(v))} end)

  defp deep_merge(a, b) when is_map(a) and is_map(b) and not is_struct(a) and not is_struct(b) do
    Map.merge(a, b, fn _k, va, vb -> deep_merge(va, vb) end)
  end

  defp deep_merge(_a, b), do: b
end
