defmodule Natsbeam.JetStream do
  @moduledoc """
  JetStream: persistent streams and consumers on top of core NATS.

  Supervises stream, consumer and per-account API processes, recovers
  file-backed streams from `store_dir` at boot, enforces account limits, and
  places replicas across the Erlang cluster.

  Configuration (`config :natsbeam, jetstream: [...]`):

    * `:enabled` — default `true`
    * `:store_dir` — default `data/jetstream` under the working directory
    * `:max_memory` / `:max_storage` — server-wide caps in bytes (-1 unlimited)
    * `:cluster_name` — reported in stream/consumer cluster info (default "natsbeam")

  Per account, JetStream is on for the default account, for static accounts
  created with `jetstream: true` (or a limits map), and for JWT accounts whose
  claims grant `mem_storage`/`disk_storage`.
  """
  use Supervisor
  require Logger

  alias Natsbeam.Config
  alias Natsbeam.JetStream.{API, Cluster, Consumer, Util}
  alias Natsbeam.JetStream.Stream, as: JStream

  def start_link(opts), do: Supervisor.start_link(__MODULE__, opts, name: __MODULE__)

  @impl true
  def init(_opts) do
    children = [
      {DynamicSupervisor, name: Natsbeam.JetStream.StreamSupervisor, strategy: :one_for_one},
      {DynamicSupervisor, name: Natsbeam.JetStream.ConsumerSupervisor, strategy: :one_for_one},
      {DynamicSupervisor, name: Natsbeam.JetStream.APISupervisor, strategy: :one_for_one},
      {Task.Supervisor, name: Natsbeam.JetStream.TaskSupervisor},
      Natsbeam.JetStream.Manager
    ]

    Supervisor.init(children, strategy: :rest_for_one)
  end

  # ------------------------------------------------------------ enablement

  def settings, do: Config.get(:jetstream) || %{}

  @doc "Is JetStream enabled on this server?"
  def enabled?, do: Map.get(settings(), :enabled, true) == true

  @doc "Is JetStream enabled for `account`?"
  def enabled_for?(account) do
    enabled?() and account_limits(account) != nil
  end

  @doc """
  JetStream limits for an account, or nil when it has no JetStream. Keys:
  `max_memory`, `max_storage`, `max_streams`, `max_consumers`, `max_bytes_required`.
  """
  def account_limits(account) do
    default = Config.get([:auth, :default_account])
    static = Config.get([:accounts, account]) || %{}

    cond do
      Map.has_key?(static, :jetstream) ->
        case static[:jetstream] do
          false -> nil
          nil -> nil
          true -> unlimited()
          map when is_map(map) -> Map.merge(unlimited(), map)
          kw when is_list(kw) -> Map.merge(unlimited(), Map.new(kw))
        end

      claims = Config.get([:auth, :account_jwts, account]) ->
        jwt_limits(claims)

      account == default ->
        unlimited()

      true ->
        nil
    end
  end

  defp unlimited,
    do: %{
      max_memory: -1,
      max_storage: -1,
      max_streams: -1,
      max_consumers: -1,
      max_bytes_required: false
    }

  defp jwt_limits(claims) do
    limits = get_in(claims, ["nats", "limits"]) || %{}

    tier =
      get_in(limits, ["tiered_limits", "R1"]) ||
        get_in(claims, ["nats", "limits", "tiered_limits", "R1"]) || %{}

    mem = tier["mem_storage"] || limits["mem_storage"] || 0
    disk = tier["disk_storage"] || limits["disk_storage"] || 0

    if mem == 0 and disk == 0 do
      nil
    else
      %{
        max_memory: mem,
        max_storage: disk,
        max_streams: tier["streams"] || limits["streams"] || -1,
        max_consumers: tier["consumer"] || limits["consumer"] || -1,
        max_bytes_required: (tier["max_bytes_required"] || limits["max_bytes_required"]) == true
      }
    end
  end

  @doc "JetStream domain, if configured (`config :natsbeam, jetstream: [domain: \"hub\"]`)."
  def domain, do: Map.get(settings(), :domain)

  def store_dir do
    Map.get(settings(), :store_dir) || Path.join(File.cwd!(), "data/jetstream")
  end

  defp account_dir(account), do: Path.join([store_dir(), safe(account), "streams"])
  defp stream_dir(account, name), do: Path.join(account_dir(account), safe(name))

  defp consumer_dir(account, stream, name),
    do: Path.join([stream_dir(account, stream), "consumers", safe(name)])

  defp safe(name), do: String.replace(name, ["/", "\\", ".."], "_")

  # -------------------------------------------------------------- starting

  @doc """
  Make sure the account's `$JS.API` responder exists and matches the current
  configuration (idempotent). An existing responder is synced in place, never
  restarted, so a JetStream-enabled account has no window without one.
  """
  def ensure_api(account, sublist \\ nil, attempts \\ 200) do
    case Registry.lookup(Natsbeam.Registry, {:js_api, account}) do
      [{pid, _}] ->
        cond do
          attempts > 0 and node(pid) == node() and not Process.alive?(pid) ->
            # A crashed responder stays registered until the registry sees its exit.
            Process.sleep(10)
            ensure_api(account, sublist, attempts - 1)

          # Called from Account.init: the responder subscribes through the
          # account, so it cannot be synced from here; the Manager syncs it on
          # the account start event.
          sublist != nil ->
            {:ok, pid}

          true ->
            try do
              API.sync_now(pid)
              {:ok, pid}
            catch
              :exit, _ when attempts > 0 ->
                Process.sleep(10)
                ensure_api(account, sublist, attempts - 1)
            end
        end

      [] ->
        case DynamicSupervisor.start_child(
               Natsbeam.JetStream.APISupervisor,
               {API, {account, sublist}}
             ) do
          {:error, {:already_started, _pid}} when attempts > 0 ->
            ensure_api(account, sublist, attempts - 1)

          {:error, {:already_started, pid}} ->
            {:ok, pid}

          other ->
            other
        end
    end
  end

  @doc false
  # Called from Account.init: start the API responder right away, subscribing
  # straight into the account's sublist so the first request never races it.
  def on_account_init(account, sublist) do
    if Process.whereis(Natsbeam.JetStream.APISupervisor) && enabled_for?(account) do
      ensure_api(account, sublist)
    end

    :ok
  end

  @doc """
  Withdraw the account's `$JS.API` responder when the account no longer has
  JetStream. The process stays (idle, unsubscribed) and resubscribes in place
  if JetStream comes back, so racing enable/disable decisions cannot leave a
  JetStream-enabled account without a responder.
  """
  def stop_api(account) do
    case Registry.lookup(Natsbeam.Registry, {:js_api, account}) do
      [{pid, _}] ->
        try do
          API.sync_now(pid)
        catch
          :exit, _ -> :ok
        end

      [] ->
        :ok
    end
  end

  @doc """
  Start a stream with this node as leader. If a leader already exists
  elsewhere in the cluster (recovery after a restart), start a local replica
  attached to it instead.
  """
  def start_stream(account, cfg, created \\ nil, opts \\ []) do
    dir = if cfg["storage"] == "file", do: stream_dir(account, cfg["name"])
    opts = Keyword.put(opts, :role, :leader)

    case DynamicSupervisor.start_child(
           Natsbeam.JetStream.StreamSupervisor,
           {JStream, {account, cfg, dir, created, opts}}
         ) do
      {:error, {:already_started, leader}} when node(leader) != node() ->
        start_replica(account, cfg, created, leader)

      other ->
        other
    end
  end

  @doc "Start a replica of a stream on this node, following `leader`."
  def start_replica(account, cfg, created, leader) do
    dir = if cfg["storage"] == "file", do: stream_dir(account, cfg["name"])

    DynamicSupervisor.start_child(
      Natsbeam.JetStream.StreamSupervisor,
      {JStream, {account, cfg, dir, created, role: :replica, leader: leader}}
    )
  end

  @doc "Start a consumer on the node that leads its stream."
  def start_consumer(account, stream, cfg, created \\ nil, saved \\ nil) do
    with {:ok, spid} <- JStream.lookup(account, stream) do
      if node(spid) == node() do
        start_consumer_local(account, stream, cfg, created, saved)
      else
        case :rpc.call(
               node(spid),
               __MODULE__,
               :start_consumer_local,
               [account, stream, cfg, created, saved],
               10_000
             ) do
          {:badrpc, reason} -> {:error, {:consumer_create_err, inspect(reason)}}
          other -> other
        end
      end
    end
  end

  @doc false
  def start_consumer_local(account, stream, cfg, created \\ nil, saved \\ nil, storage \\ nil) do
    storage =
      storage ||
        case JStream.lookup(account, stream) do
          {:ok, spid} -> JStream.config(spid)["storage"]
          _ -> nil
        end

    dir =
      if storage == "file" and is_binary(cfg["durable_name"]),
        do: consumer_dir(account, stream, cfg["name"]),
        else: nil

    DynamicSupervisor.start_child(
      Natsbeam.JetStream.ConsumerSupervisor,
      {Consumer, {account, stream, cfg, dir, created, saved}}
    )
  end

  @doc "Restore a stream from a snapshot archive (see `Natsbeam.JetStream.Stream.snapshot/2`)."
  def restore_stream(account, cfg, archive) do
    with {:ok, cfg} <- JStream.normalize_config(cfg) do
      case JStream.lookup(account, cfg["name"]) do
        {:ok, _} -> {:error, :stream_name_in_use}
        {:error, _} -> do_restore(account, cfg, archive)
      end
    end
  end

  defp do_restore(account, cfg, archive) do
    with {:ok, pid} <- start_stream(account, cfg),
         {:ok, info} <- JStream.load_snapshot(pid, archive) do
      {:ok, info}
    else
      {:error, {:already_started, pid}} -> JStream.load_snapshot(pid, archive)
      {:error, reason} when is_atom(reason) -> {:error, reason}
      {:error, {reason, detail}} when is_atom(reason) -> {:error, {reason, detail}}
      {:error, reason} -> {:error, {:stream_restore_err, inspect(reason)}}
    end
  end

  # ---------------------------------------------------------------- limits

  @doc false
  def check_stream_limits(account, cfg) do
    limits = account_limits(account) || unlimited()
    streams = JStream.list(account)
    usage = usage(account)
    max_streams = limits.max_streams
    nodes = length(Cluster.nodes())

    cond do
      is_integer(max_streams) and max_streams >= 0 and length(streams) >= max_streams and
          cfg["name"] not in streams ->
        {:error, :maximum_streams_limit}

      limits.max_bytes_required and (cfg["max_bytes"] || -1) <= 0 ->
        {:error, :stream_max_bytes_required}

      cfg["storage"] == "memory" and limits.max_memory >= 0 and usage.memory >= limits.max_memory ->
        {:error, :memory_resources_exceeded}

      cfg["storage"] == "file" and limits.max_storage >= 0 and usage.storage >= limits.max_storage ->
        {:error, :storage_resources_exceeded}

      cfg["num_replicas"] > nodes ->
        {:error,
         {:cluster_no_peers,
          "#{cfg["num_replicas"]} replicas requested, #{nodes} JetStream node(s) available"}}

      true ->
        :ok
    end
  end

  @doc false
  def check_consumer_limits(account) do
    limits = account_limits(account) || unlimited()
    max = limits.max_consumers

    total = Enum.sum(for s <- JStream.list(account), do: length(Consumer.list(account, s)))

    if is_integer(max) and max >= 0 and total >= max,
      do: {:error, :maximum_consumers_limit},
      else: :ok
  end

  @doc "Memory/storage bytes and counts used by an account (cluster-wide)."
  def usage(account) do
    streams = JStream.list(account)

    {mem, disk, consumers} =
      Enum.reduce(streams, {0, 0, 0}, fn name, {m, d, c} ->
        with {:ok, pid} <- JStream.lookup(account, name),
             %{} = u <- safe_call(pid, :usage) do
          c2 = c + length(Consumer.list(account, name))
          if u.storage == "memory", do: {m + u.bytes, d, c2}, else: {m, d + u.bytes, c2}
        else
          _ -> {m, d, c}
        end
      end)

    %{memory: mem, storage: disk, streams: length(streams), consumers: consumers}
  end

  defp safe_call(pid, msg) do
    GenServer.call(pid, msg, 5_000)
  catch
    :exit, _ -> nil
  end

  @doc "The `$JS.API.INFO` payload for an account."
  def account_info(account) do
    limits = account_limits(account) || unlimited()
    u = usage(account)

    api =
      case Registry.lookup(Natsbeam.Registry, {:js_api, account}) do
        [{pid, _}] when pid != self() -> GenServer.call(pid, :api_stats)
        [{_pid, _}] -> Process.get(:api_stats, %{"total" => 0, "errors" => 0})
        [] -> %{"total" => 0, "errors" => 0}
      end

    %{
      "memory" => u.memory,
      "storage" => u.storage,
      "reserved_memory" => 0,
      "reserved_storage" => 0,
      "streams" => u.streams,
      "consumers" => u.consumers,
      "limits" => %{
        "max_memory" => limits.max_memory,
        "max_storage" => limits.max_storage,
        "max_streams" => limits.max_streams,
        "max_consumers" => limits.max_consumers,
        "max_ack_pending" => -1,
        "memory_max_stream_bytes" => -1,
        "storage_max_stream_bytes" => -1,
        "max_bytes_required" => limits.max_bytes_required
      },
      "api" => Map.put(api, "level", 1)
    }
    |> then(fn info -> if d = domain(), do: Map.put(info, "domain", d), else: info end)
  end

  @doc "Streams in an account with their info."
  def streams(account) do
    for name <- JStream.list(account),
        {:ok, pid} <- [JStream.lookup(account, name)],
        info = safe_call(pid, {:info, %{}}),
        info != nil,
        do: info
  end

  @doc "Server-level JetStream statistics (for `$SYS.REQ.SERVER.*.JSZ`)."
  def jsz do
    accounts = Enum.uniq(for {a, _} <- Cluster.all_streams(), do: a)

    totals =
      Enum.reduce(
        accounts,
        %{streams: 0, consumers: 0, messages: 0, bytes: 0, memory: 0, storage: 0},
        fn a, acc ->
          u = usage(a)

          msgs =
            Enum.sum(
              for name <- JStream.list(a),
                  {:ok, pid} <- [JStream.lookup(a, name)],
                  %{} = us <- [safe_call(pid, :usage)],
                  do: us.msgs
            )

          %{
            acc
            | streams: acc.streams + u.streams,
              consumers: acc.consumers + u.consumers,
              messages: acc.messages + msgs,
              bytes: acc.bytes + u.memory + u.storage,
              memory: acc.memory + u.memory,
              storage: acc.storage + u.storage
          }
        end
      )

    nodes = Cluster.nodes()

    %{
      "server_id" => Natsbeam.Server.info().id,
      "now" => Util.ts(Util.now_ns()),
      "disabled" => not enabled?(),
      "config" => %{
        "max_memory" => Map.get(settings(), :max_memory, -1),
        "max_storage" => Map.get(settings(), :max_storage, -1),
        "store_dir" => store_dir()
      },
      "memory" => totals.memory,
      "storage" => totals.storage,
      "reserved_memory" => 0,
      "reserved_storage" => 0,
      "accounts" => length(accounts),
      "ha_assets" =>
        Enum.count(Cluster.all_streams(), fn {a, n} ->
          case JStream.lookup(a, n) do
            {:ok, pid} -> (safe_call(pid, :config) || %{})["num_replicas"] > 1
            _ -> false
          end
        end),
      "api" => %{"total" => 0, "errors" => 0},
      "streams" => totals.streams,
      "consumers" => totals.consumers,
      "messages" => totals.messages,
      "bytes" => totals.bytes,
      "meta_cluster" => %{
        "name" => Cluster.cluster_name(),
        "leader" => Cluster.server_name(hd(nodes)),
        "peer" => Cluster.server_name(),
        "cluster_size" => length(nodes),
        "replicas" =>
          for n <- nodes, n != hd(nodes) do
            %{
              "name" => Cluster.server_name(n),
              "current" => true,
              "active" => 0,
              "offline" => false,
              "lag" => 0,
              "peer" => Cluster.server_name(n)
            }
          end
      }
    }
  end

  # -------------------------------------------------------------- recovery

  @doc false
  def recover do
    dir = store_dir()

    if enabled?() and File.dir?(dir) do
      for account <- File.ls!(dir), File.dir?(Path.join([dir, account, "streams"])) do
        Natsbeam.Account.ensure(account)

        for sname <- File.ls!(Path.join([dir, account, "streams"])) do
          sdir = Path.join([dir, account, "streams", sname])
          recover_stream(account, sdir)
        end
      end
    end

    :ok
  end

  defp recover_stream(account, sdir) do
    with {:ok, body} <- File.read(Path.join(sdir, "meta.json")),
         {:ok, %{"config" => cfg} = meta} <- Jason.decode(body),
         {:ok, cfg} <- JStream.normalize_config(cfg) do
      case start_stream(account, cfg, meta["created"], recovered: true) do
        {:ok, pid} ->
          role =
            if node(pid) == node() and Cluster.whereis(JStream.key(account, cfg["name"])) == pid,
              do: "leader",
              else: "replica"

          Logger.info("natsbeam: recovered stream #{account}/#{cfg["name"]} as #{role}")

        {:error, reason} ->
          Logger.error("natsbeam: could not recover stream #{sdir}: #{inspect(reason)}")
      end
    else
      err -> Logger.error("natsbeam: skipping stream dir #{sdir}: #{inspect(err)}")
    end
  end

  @doc false
  def ts_now, do: Util.ts(Util.now_ns())
end

defmodule Natsbeam.JetStream.Manager do
  @moduledoc false
  # Enables the API for accounts as they start, joins the cluster node group
  # and recovers persisted streams at boot.
  use GenServer
  alias Natsbeam.JetStream
  alias Natsbeam.JetStream.Cluster

  def start_link(opts), do: GenServer.start_link(__MODULE__, opts, name: __MODULE__)

  @impl true
  def init(_) do
    # Handlers outlive a crashed Manager; re-attach them to this process.
    for id <- [:config, :account_start], do: :telemetry.detach({__MODULE__, id})

    :telemetry.attach(
      {__MODULE__, :config},
      [:natsbeam, :config, :changed],
      fn _, _, _, pid -> send(pid, :config_changed) end,
      self()
    )

    # A restarted account has a fresh sublist: resubscribe its responder.
    :telemetry.attach(
      {__MODULE__, :account_start},
      [:natsbeam, :account, :start],
      fn _, _, %{account: name}, pid -> send(pid, {:account_started, name}) end,
      self()
    )

    if JetStream.enabled?(), do: Cluster.join_nodes_group()
    {:ok, %{}, {:continue, :recover}}
  end

  @impl true
  def handle_continue(:recover, state) do
    # Give freshly connected peers a moment so recovery sees existing leaders.
    if Cluster.peers() != [], do: :global.sync()
    JetStream.recover()
    for account <- Natsbeam.Account.list(), do: sync_account(account)
    {:noreply, state}
  end

  @impl true
  def handle_info({:account_started, account}, state) do
    sync_account(account)
    {:noreply, state}
  end

  def handle_info(:config_changed, state) do
    for account <- Natsbeam.Account.list(), do: sync_account(account)
    {:noreply, state}
  end

  def handle_info(_, state), do: {:noreply, state}

  defp sync_account(account) do
    if JetStream.enabled_for?(account),
      do: JetStream.ensure_api(account),
      else: JetStream.stop_api(account)
  end
end
