defmodule Natsbeam do
  @moduledoc """
  A NATS-compatible messaging server on the BEAM.

  Everything here is live: listeners, accounts, users, NKeys, operator JWTs and
  auth providers can be added or removed while clients are connected, with the
  same effect `nats-server --signal reload` would have, but as plain function
  calls.

  ## Quick start

      iex> {:ok, _} = Natsbeam.start_listener(port: 4222)
      iex> Natsbeam.add_user("alice", password: "s3cret", account: "ACME")
      iex> # ... nats --user alice --password s3cret pub hello world

  ## In-VM clients

  Elixir processes can take part without a socket:

      Natsbeam.subscribe("greetings.*", account: "ACME")
      Natsbeam.publish("greetings.en", "hello", account: "ACME")
      receive do
        {:natsbeam_msg, _sid, subject, _reply, _headers, payload} -> {subject, payload}
      end
  """
  alias Natsbeam.{Account, Auth, Clients, Config, JWT, Listener, Local, NKeys}

  # ------------------------------------------------------------- listeners

  @doc "Start a listener. See `Natsbeam.Listener` for options."
  @spec start_listener(keyword() | map()) :: {:ok, pid()} | {:error, term()}
  def start_listener(opts \\ []) do
    DynamicSupervisor.start_child(Natsbeam.ListenerSupervisor, Listener.child_spec(opts))
  end

  @doc "Stop a listener by name. Existing connections on it are closed."
  def stop_listener(name) do
    case Registry.lookup(Natsbeam.Registry, {:listener, name}) do
      [{pid, _}] -> DynamicSupervisor.terminate_child(Natsbeam.ListenerSupervisor, pid)
      [] -> {:error, :not_found}
    end
  end

  @doc "Running listeners as `%{name => port}`."
  def listeners do
    Map.new(Listener.list(), fn name ->
      {:ok, port} = Listener.port(name)
      {name, port}
    end)
  end

  # -------------------------------------------------------------- accounts

  @doc """
  Create (or reconfigure) an account. Options: `:max_connections`,
  `:max_subscriptions`, `:max_payload`. Accounts also spring into existence
  the first time an identity references them.
  """
  def add_account(name, opts \\ []) do
    name = to_string(name)
    Config.put_in([:accounts, name], Map.new(opts))
    {:ok, pid, _} = Account.ensure(name)

    if Natsbeam.JetStream.enabled_for?(name),
      do: Natsbeam.JetStream.ensure_api(name),
      else: Natsbeam.JetStream.stop_api(name)

    {:ok, pid}
  end

  @doc "Remove an account, disconnecting every client in it."
  def remove_account(name) do
    name = to_string(name)
    Config.update_in([:accounts], &Map.delete(&1, name))
    Account.remove(name)
  end

  @doc "Running accounts and their stats."
  def accounts, do: Account.list() |> Enum.map(&Account.stats/1)

  # ----------------------------------------------------------------- users

  @doc """
  Add a username/password user. The password may be a bcrypt hash (as produced
  by `nats server passwd` or `Natsbeam.hash_password/1`).

  Options: `:account` (default `"$G"`), `:permissions` (map with
  `publish`/`subscribe` allow/deny lists and optional `response`),
  `:max_payload`, `:max_subscriptions`.
  """
  def add_user(name, opts) do
    name = to_string(name)

    spec =
      opts
      |> Map.new()
      |> Map.put_new(:account, Config.get([:auth, :default_account]))
      |> Auth.normalize_user()

    Config.put_in([:auth, :users, name], spec)
    :ok
  end

  @doc "Remove a user; its live connections are closed with an authorization error."
  def remove_user(name) do
    name = to_string(name)
    Config.update_in([:auth, :users], &Map.delete(&1, name))
    kick(fn c -> c.kind == :password and c.user == name end)
  end

  @doc "Configured password users."
  def users, do: Config.get([:auth, :users])

  @doc "Add an NKey user (public key `U...`). Same options as `add_user/2`."
  def add_nkey(public_key, opts \\ []) do
    unless NKeys.valid_public?(public_key, :user),
      do: raise(ArgumentError, "not a user public nkey")

    spec =
      opts
      |> Map.new()
      |> Map.put_new(:account, Config.get([:auth, :default_account]))
      |> Auth.normalize_user()

    Config.put_in([:auth, :nkeys, public_key], spec)
    :ok
  end

  def remove_nkey(public_key) do
    Config.update_in([:auth, :nkeys], &Map.delete(&1, public_key))
    kick(fn c -> c.kind == :nkey and c.nkey == public_key end)
  end

  def nkeys, do: Config.get([:auth, :nkeys])

  @doc "Set (or clear with `nil`) the shared auth token. Clearing kicks token clients."
  def set_token(token) do
    Config.put_in([:auth, :token], token)
    if token == nil, do: kick(fn c -> c.kind == :token end), else: :ok
  end

  @doc "Bcrypt-hash a secret for use in `add_user/2` or `set_token/1`."
  def hash_password(plain), do: Bcrypt.hash_pwd_salt(plain, log_rounds: 11)

  # -------------------------------------------------------------- operator

  @doc """
  Trust an operator JWT (switches the server into operator/JWT mode). Accepts
  the raw JWT string. Also loads the operator's `system_account` if set.
  """
  def add_operator(jwt) do
    with {:ok, claims} <- JWT.verify(jwt),
         "operator" <- JWT.type(claims) || {:error, :not_operator_jwt},
         true <- claims["iss"] == claims["sub"] || {:error, :operator_not_self_signed} do
      Config.update(fn cfg ->
        cfg
        |> put_in([:auth, :operators, claims["sub"]], claims)
        |> then(fn c ->
          case get_in(claims, ["nats", "system_account"]) do
            nil -> c
            sys -> put_in(c, [:auth, :system_account], sys)
          end
        end)
      end)

      {:ok, claims}
    else
      {:error, _} = e -> e
      _ -> {:error, :not_operator_jwt}
    end
  end

  def remove_operator(public_key) do
    Config.update_in([:auth, :operators], &Map.delete(&1, public_key))
    kick(fn c -> c.kind == :jwt end)
  end

  def operators, do: Config.get([:auth, :operators]) |> Map.keys()

  @doc """
  Add/replace an account JWT in the memory resolver. The JWT must be signed by
  a trusted operator. Clients already connected to the account keep their
  session unless the account is now expired or their user was revoked.
  """
  def add_account_jwt(jwt) do
    auth = Config.get(:auth)

    with {:ok, claims} <- JWT.verify(jwt),
         "account" <- JWT.type(claims) || {:error, :not_account_jwt},
         :ok <- Auth.check_account_trusted(claims, auth) do
      Config.update(fn cfg ->
        cfg
        |> put_in([:auth, :account_jwts, claims["sub"]], claims)
        |> update_in(
          [:auth],
          &Map.put(&1, :account_jwt_raw, Map.put(&1[:account_jwt_raw] || %{}, claims["sub"], jwt))
        )
      end)

      apply_account_revocations(claims)
      {:ok, claims}
    else
      {:error, _} = e -> e
      _ -> {:error, :not_account_jwt}
    end
  end

  @doc "Forget an account JWT and disconnect its clients."
  def remove_account_jwt(public_key) do
    Config.update(fn cfg ->
      cfg
      |> update_in([:auth, :account_jwts], &Map.delete(&1, public_key))
      |> update_in(
        [:auth],
        &Map.put(&1, :account_jwt_raw, Map.delete(&1[:account_jwt_raw] || %{}, public_key))
      )
    end)

    Account.remove(public_key)
  end

  def account_jwts, do: Config.get([:auth, :account_jwts]) |> Map.keys()

  defp apply_account_revocations(claims) do
    revs = get_in(claims, ["nats", "revocations"]) || %{}

    if revs != %{} do
      kick(fn c ->
        c.account == claims["sub"] and c.kind == :jwt and
          Enum.any?([c.nkey, "*"], fn k -> is_integer(revs[k]) end)
      end)
    end

    :ok
  end

  @doc """
  Name the system account (static mode; `nil` disables it). The default is
  `"$SYS"`, nats-server's own default; when joining Go servers configured with
  `system_account: SYS`, use the same name (`"SYS"`) so the account shares
  their pinned route. In operator mode it is taken from the operator JWT.
  `Natsbeam.SystemAccount` then answers `$SYS.REQ.*` requests and sends the
  STATSZ/SHUTDOWN events there. New route connections pick up the name; the
  pinned route of a live route is not re-opened.
  """
  def set_system_account(name) do
    Config.put_in([:auth, :system_account], name && to_string(name))
    :ok
  end

  @doc "Install a custom `Natsbeam.Auth.Provider` (or `nil` to remove it)."
  def set_auth_provider(nil) do
    Config.put_in([:auth, :provider], nil)
    kick(fn c -> c.kind == :custom end)
  end

  def set_auth_provider(module, opts \\ []) when is_atom(module) do
    Config.put_in([:auth, :provider], {module, opts})
    :ok
  end

  # --------------------------------------------------------------- clients

  @doc "Connected clients."
  defdelegate clients, to: Clients, as: :list

  @doc "Disconnect a client by id or pid."
  defdelegate disconnect(id_or_pid, reason \\ "Connection Closed by Server"), to: Clients

  defp kick(pred) do
    Clients.filter(pred) |> Enum.each(&Clients.disconnect(&1.pid, "Authorization Violation"))
    :ok
  end

  # ------------------------------------------------------------ in-VM client

  @doc "Subscribe the calling process. See `Natsbeam.Local.subscribe/2`."
  defdelegate subscribe(subject, opts \\ []), to: Local
  defdelegate unsubscribe(sid, opts \\ []), to: Local
  defdelegate publish(subject, payload, opts \\ []), to: Local
  defdelegate request(subject, payload, opts \\ []), to: Local

  # ----------------------------------------------------------------- misc

  @doc "Server identity and runtime stats."
  def info do
    srv = Natsbeam.Server.info()

    %{
      server_id: srv.id,
      server_name: srv.name,
      version: srv.version,
      started_at: srv.started_at,
      listeners: listeners(),
      accounts: Account.list(),
      clients: Clients.count(),
      auth_required: Auth.required?(),
      operator_mode: Auth.operator_mode?(),
      jetstream: Natsbeam.JetStream.enabled?(),
      node: node()
    }
  end

  # ----------------------------------------------------------- links

  @doc """
  Join a nats-server cluster as a route: `Natsbeam.add_route("nats-route://host:6222")`.
  The cluster name (`config :natsbeam, cluster_name:`) must match the peer's.
  Options: `:user`/`:pass`.
  """
  def add_route(url, opts \\ []), do: Natsbeam.Link.connect(:route, url, opts)

  @doc """
  Connect to a nats-server leafnode port as a spoke:
  `Natsbeam.add_leaf_remote("nats-leaf://host:7422", account: "$G", credentials: "user.creds")`.
  """
  def add_leaf_remote(url, opts \\ []), do: Natsbeam.Link.connect(:leaf, url, opts)

  @doc "Accept route connections from other servers on `port` (our cluster port)."
  def start_route_listener(port, opts \\ []) do
    Config.put(:cluster_port, port)

    DynamicSupervisor.start_child(
      Natsbeam.AcceptorSupervisor,
      {Natsbeam.Link.Acceptor, {:route, port, opts}}
    )
  end

  @doc "Accept leafnode connections (nats-server `leafnodes.remotes`) on `port`."
  def start_leaf_listener(port, opts \\ []) do
    Config.put(:leaf_port, port)

    DynamicSupervisor.start_child(
      Natsbeam.AcceptorSupervisor,
      {Natsbeam.Link.Acceptor, {:leaf, port, opts}}
    )
  end

  @doc """
  Connect to another cluster's gateway port:
  `Natsbeam.add_gateway("nats-gateway://host:7222", name: "remote-cluster")`.
  `:name` is the remote gateway (cluster) name; ours is our cluster name.
  The remote dials back to our gateway listener (`start_gateway_listener/2`),
  using the URL we advertise, if it is not configured with it already.
  Options: `:user`/`:pass` (or in the URL), `:tls`.
  """
  def add_gateway(url, opts) do
    Natsbeam.Gateway.connect(url, opts)
  end

  @doc "Close both connections with the gateway `name` (an explicit one is not redialled)."
  defdelegate remove_gateway(name), to: Natsbeam.Gateway, as: :remove

  @doc """
  Accept gateway connections from other clusters on `port` (0 = random).
  Options: `:host` (default "0.0.0.0").
  """
  def start_gateway_listener(port, opts \\ []) do
    Config.put(:gateway_port, port)
    if h = opts[:host], do: Config.put(:gateway_host, h)

    case DynamicSupervisor.start_child(
           Natsbeam.AcceptorSupervisor,
           {Natsbeam.Link.Acceptor, {:gateway, port, opts}}
         ) do
      {:ok, pid} ->
        {:ok, actual} = Natsbeam.Link.Acceptor.port(:gateway)
        Config.put(:gateway_port, actual)
        {:ok, pid}

      other ->
        other
    end
  end

  @doc "Status of every gateway connection (inbound and outbound)."
  defdelegate gateways, to: Natsbeam.Gateway, as: :list

  @doc "Status of every route/leaf/gateway link."
  defdelegate links, to: Natsbeam.Link, as: :list

  @doc "JetStream streams in an account."
  def streams(account \\ nil),
    do: Natsbeam.JetStream.streams(to_string(account || Config.get([:auth, :default_account])))

  @doc "JetStream usage and limits for an account."
  def jetstream_info(account \\ nil),
    do:
      Natsbeam.JetStream.account_info(to_string(account || Config.get([:auth, :default_account])))

  @doc "The live configuration map."
  defdelegate config, to: Config, as: :all
end
