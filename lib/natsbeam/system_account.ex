defmodule Natsbeam.SystemAccount do
  @moduledoc """
  Answers `$SYS.REQ.*` requests inside the system account, the way nats-server's
  internal system client does, so operator tooling works unchanged:

    * `nsc push` — `$SYS.REQ.CLAIMS.UPDATE`, `$SYS.REQ.ACCOUNT.<id>.CLAIMS.UPDATE`
    * `nsc pull` / lookups — `$SYS.REQ.ACCOUNT.<id>.CLAIMS.LOOKUP`, `$SYS.REQ.CLAIMS.LIST`
    * `nsc push --prune` — `$SYS.REQ.CLAIMS.DELETE` (operator-signed generic JWT)
    * `nats server ping|list|info|report` — `$SYS.REQ.SERVER.PING`,
      `$SYS.REQ.SERVER.PING.<Z>` and `$SYS.REQ.SERVER.<id>.<Z>` for `STATSZ`,
      `IDZ`, `VARZ`, `CONNZ`, `ROUTEZ`, `LEAFZ`, `SUBSZ`, `GATEWAYZ`,
      `HEALTHZ`, `ACCOUNTZ` and `JSZ`, honouring the `server_name`, `cluster`,
      `host`, `domain`, `tags` and `exact_match` filters of the request body

  Like a Go server it also publishes `$SYS.SERVER.<id>.STATSZ` every
  `:statsz_interval` ms (starting at 250 ms and doubling up to it, as
  nats-server does), answers a reply-less `$SYS.REQ.SERVER.PING` with that
  broadcast, tracks other servers from their STATSZ/SHUTDOWN events (for
  `active_servers`) and sends `$SYS.SERVER.<id>.SHUTDOWN` when the application
  stops. All of this is ordinary interest in the system account, so requests
  from Go servers arrive over routes (the system account's pinned route) and
  over leafnodes bound to the system account, and the responses go back the
  same way.

  It is an in-VM subscriber (see `Natsbeam.Local`), so it needs no socket. It
  follows the live `[:auth, :system_account]` setting and re-subscribes when
  that changes.
  """
  use GenServer
  require Logger
  alias Natsbeam.{Config, JWT, Local}

  @z ~w(STATSZ IDZ VARZ CONNZ ROUTEZ LEAFZ SUBSZ GATEWAYZ HEALTHZ ACCOUNTZ JSZ)

  @subjects [
    "$SYS.REQ.CLAIMS.UPDATE",
    "$SYS.REQ.CLAIMS.LIST",
    "$SYS.REQ.CLAIMS.DELETE",
    "$SYS.REQ.ACCOUNT.*.CLAIMS.UPDATE",
    "$SYS.REQ.ACCOUNT.*.CLAIMS.LOOKUP",
    "$SYS.REQ.SERVER.PING",
    "$SYS.REQ.SERVER.PING.*",
    # other servers' heartbeats and goodbyes
    "$SYS.SERVER.*.STATSZ",
    "$SYS.SERVER.*.SHUTDOWN"
  ]

  # Requests aimed at this server only, like nats-server's
  # `$SYS.REQ.SERVER.<own id>.<Z>` (a `*` would also match `PING.<Z>` and
  # answer those twice).
  defp subjects do
    id = Natsbeam.Server.info().id
    @subjects ++ for(z <- @z, do: "$SYS.REQ.SERVER.#{id}.#{z}")
  end

  # nats-server forgets a remote server after 5 missed 30s heartbeats.
  @orphan_ms 150_000

  def start_link(opts), do: GenServer.start_link(__MODULE__, opts, name: __MODULE__)

  @doc """
  Publish `$SYS.SERVER.<id>.SHUTDOWN` and wait until the links have written
  it. Called from the application's `prep_stop/1`, while links are still up.
  """
  def announce_shutdown do
    GenServer.call(__MODULE__, :announce_shutdown, 5_000)
  catch
    :exit, _ -> :ok
  end

  @doc "Other servers seen through `$SYS.SERVER.<id>.STATSZ`: `%{id => server_info_map}`."
  def remote_servers, do: GenServer.call(__MODULE__, :remote_servers)

  @impl true
  def init(_) do
    :telemetry.attach(
      {__MODULE__, :config},
      [:natsbeam, :config, :changed],
      &__MODULE__.on_config_changed/4,
      self()
    )

    {:ok, %{account: nil, sids: [], servers: %{}, timer: nil, interval: 250},
     {:continue, :resubscribe}}
  end

  @doc false
  def on_config_changed(_event, _measurements, %{old: old, new: new}, pid) do
    if get_in(old, [:auth, :system_account]) != get_in(new, [:auth, :system_account]) do
      send(pid, :resubscribe)
    end
  end

  @impl true
  def handle_continue(:resubscribe, state), do: {:noreply, resubscribe(state)}

  @impl true
  def handle_call(:announce_shutdown, _from, %{account: nil} = state), do: {:reply, :ok, state}

  def handle_call(:announce_shutdown, _from, state) do
    Local.publish(
      "$SYS.SERVER.#{Natsbeam.Server.info().id}.SHUTDOWN",
      Jason.encode!(server_info()),
      account: state.account
    )

    # Links handle their mailbox in order, so once each has answered a status
    # call the event is on its socket.
    Natsbeam.Link.list()
    {:reply, :ok, cancel_timer(state)}
  end

  def handle_call(:remote_servers, _from, state),
    do: {:reply, Map.new(state.servers, fn {id, {info, _}} -> {id, info} end), state}

  @impl true
  def handle_info(:resubscribe, state), do: {:noreply, resubscribe(state)}

  def handle_info(:statsz, %{account: nil} = state), do: {:noreply, %{state | timer: nil}}

  def handle_info(:statsz, state) do
    state = prune(state)
    broadcast_statsz(state)
    interval = min(state.interval * 2, statsz_interval())
    {:noreply, schedule(%{state | interval: interval}, interval)}
  end

  def handle_info({:natsbeam_msg, _sid, "$SYS.SERVER." <> rest, _reply, _h, payload}, state),
    do: {:noreply, track_server(state, rest, payload)}

  def handle_info({:natsbeam_msg, _sid, subject, reply, _headers, payload}, state) do
    case handle_request(subject, payload, state) do
      :statsz when reply in [nil, ""] ->
        # nats-server: no reply means "send your regular STATSZ broadcast now"
        broadcast_statsz(state)

      _ when reply in [nil, ""] ->
        :ok

      :statsz ->
        Local.publish(reply, envelope(%{"statsz" => statsz(state)}), account: state.account)

      response when is_binary(response) ->
        Local.publish(reply, response, account: state.account)

      _ ->
        :ok
    end

    {:noreply, state}
  rescue
    e ->
      Logger.warning("natsbeam: $SYS request #{subject} failed: #{Exception.message(e)}")
      {:noreply, state}
  end

  def handle_info(_, state), do: {:noreply, state}

  defp resubscribe(state) do
    for sid <- state.sids, do: Local.unsubscribe(sid, account: state.account)
    account = Config.get([:auth, :system_account])

    sids =
      if account do
        Logger.info("natsbeam: serving $SYS requests in system account #{account}")
        for s <- subjects(), {:ok, sid} = Local.subscribe(s, account: account), do: sid
      else
        []
      end

    state = cancel_timer(%{state | account: account, sids: sids, servers: %{}, interval: 250})
    if account, do: schedule(state, 250), else: state
  end

  defp schedule(state, ms), do: %{state | timer: Process.send_after(self(), :statsz, ms)}

  defp cancel_timer(%{timer: nil} = state), do: state

  defp cancel_timer(state) do
    Process.cancel_timer(state.timer)
    %{state | timer: nil}
  end

  defp statsz_interval do
    case Config.get(:statsz_interval) do
      n when is_integer(n) and n > 0 -> n
      _ -> 10_000
    end
  end

  defp broadcast_statsz(state) do
    Local.publish(
      "$SYS.SERVER.#{Natsbeam.Server.info().id}.STATSZ",
      envelope(%{"statsz" => statsz(state)}),
      account: state.account
    )
  end

  # Remember (or forget) other servers from their STATSZ / SHUTDOWN events.
  defp track_server(state, rest, payload) do
    own = Natsbeam.Server.info().id

    case String.split(rest, ".") do
      [^own | _] ->
        state

      [id, "STATSZ"] ->
        info =
          case Jason.decode(payload) do
            {:ok, %{"server" => %{} = si}} -> si
            _ -> %{"id" => id}
          end

        %{state | servers: Map.put(state.servers, id, {info, now_ms()})}

      [id, "SHUTDOWN"] ->
        %{state | servers: Map.delete(state.servers, id)}

      _ ->
        state
    end
  end

  defp prune(state) do
    cutoff = now_ms() - @orphan_ms
    %{state | servers: Map.filter(state.servers, fn {_, {_, at}} -> at >= cutoff end)}
  end

  defp now_ms, do: System.monotonic_time(:millisecond)

  # ----------------------------------------------------------------- claims

  defp handle_request("$SYS.REQ.CLAIMS.UPDATE", jwt, _), do: update_claims(jwt)

  defp handle_request("$SYS.REQ.ACCOUNT." <> rest, payload, _) do
    case String.split(rest, ".") do
      [_id, "CLAIMS", "UPDATE"] -> update_claims(payload)
      [id, "CLAIMS", "LOOKUP"] -> Map.get(Config.get([:auth, :account_jwt_raw]) || %{}, id, "")
      _ -> nil
    end
  end

  defp handle_request("$SYS.REQ.CLAIMS.LIST", _payload, _) do
    Jason.encode!(Natsbeam.account_jwts())
  end

  defp handle_request("$SYS.REQ.CLAIMS.DELETE", jwt, _) do
    with {:ok, claims} <- JWT.verify(String.trim(jwt)),
         true <-
           Map.has_key?(Config.get([:auth, :operators]), claims["iss"]) || {:error, :untrusted},
         accounts when is_list(accounts) <-
           get_in(claims, ["nats", "accounts"]) || {:error, :no_accounts} do
      Enum.each(accounts, &Natsbeam.remove_account_jwt/1)
      envelope(%{"data" => %{"code" => 200, "message" => "deleted #{length(accounts)} accounts"}})
    else
      {:error, reason} ->
        envelope(%{
          "error" => %{"code" => 400, "description" => "delete failed: #{inspect(reason)}"}
        })
    end
  end

  # ----------------------------------------------------------------- server

  defp handle_request("$SYS.REQ.SERVER.PING", payload, state),
    do: server_request("STATSZ", payload, state)

  defp handle_request("$SYS.REQ.SERVER.PING." <> z, payload, state),
    do: server_request(z, payload, state)

  defp handle_request("$SYS.REQ.SERVER." <> rest, payload, state) do
    srv = Natsbeam.Server.info()

    case String.split(rest, ".") do
      [id, z] when id == srv.id -> server_request(z, payload, state)
      _ -> nil
    end
  end

  defp handle_request(_, _, _), do: nil

  defp server_request(z, payload, state) when z in @z do
    case filter_opts(payload) do
      {:error, msg} ->
        envelope(%{"error" => %{"code" => 400, "description" => msg}})

      {:ok, opts} ->
        cond do
          not applies?(opts) -> nil
          z == "STATSZ" -> :statsz
          z == "IDZ" -> idz()
          true -> envelope(%{"data" => z_data(z, opts, state)})
        end
    end
  end

  defp server_request(_z, _payload, _state), do: nil

  defp z_data("VARZ", _opts, _state), do: varz()
  defp z_data("CONNZ", opts, _state), do: connz(opts)
  defp z_data("ROUTEZ", _opts, _state), do: routez()
  defp z_data("LEAFZ", _opts, _state), do: leafz()
  defp z_data("SUBSZ", _opts, _state), do: subsz()
  defp z_data("GATEWAYZ", _opts, _state), do: gatewayz()
  defp z_data("HEALTHZ", _opts, _state), do: %{"status" => "ok", "status_code" => 200}
  defp z_data("ACCOUNTZ", _opts, _state), do: accountz()
  defp z_data("JSZ", _opts, _state), do: Natsbeam.JetStream.jsz()

  defp filter_opts(payload) do
    case String.trim(payload || "") do
      "" ->
        {:ok, %{}}

      body ->
        case Jason.decode(body) do
          {:ok, %{} = opts} -> {:ok, opts}
          {:ok, _} -> {:ok, %{}}
          {:error, e} -> {:error, Exception.message(e)}
        end
    end
  end

  # nats-server's filterRequest/1: does a request with these EventFilterOptions
  # concern this server?
  defp applies?(opts) do
    srv = Natsbeam.Server.info()
    exact? = opts["exact_match"] == true

    matches? = fn
      nil, _ -> true
      "", _ -> true
      want, have when exact? -> want == have
      want, have -> String.contains?(have || "", want)
    end

    tags = Config.get(:server_tags) || []
    domain = opts["domain"]

    matches?.(opts["server_name"], srv.name) and matches?.(opts["host"], host()) and
      matches?.(opts["cluster"], cluster_name()) and
      Enum.all?(opts["tags"] || [], &(&1 in tags)) and
      (domain in [nil, ""] or domain == Natsbeam.JetStream.domain())
  end

  defp update_claims(jwt) do
    jwt = String.trim(jwt)

    case Natsbeam.add_account_jwt(jwt) do
      {:ok, claims} ->
        envelope(%{
          "data" => %{"account" => claims["sub"], "code" => 200, "message" => "jwt updated"}
        })

      {:error, reason} ->
        account =
          case JWT.decode(jwt) do
            {:ok, c, _} -> c["sub"]
            _ -> nil
          end

        envelope(%{
          "error" => %{
            "account" => account,
            "code" => 400,
            "description" => "jwt update rejected: #{inspect(reason)}"
          }
        })
    end
  end

  # ------------------------------------------------------------------ data

  defp envelope(map) do
    Jason.encode!(Map.put(map, "server", server_info()))
  end

  # nats-server's ServerInfo. `seq` grows with every message we send, which Go
  # servers check on STATSZ heartbeats.
  defp server_info do
    srv = Natsbeam.Server.info()
    cfg = Config.all()
    js? = Natsbeam.JetStream.enabled?()

    %{
      "name" => srv.name,
      "host" => host(),
      "id" => srv.id,
      "cluster" => cluster_name(),
      "domain" => Natsbeam.JetStream.domain(),
      "ver" => cfg.advertised_version,
      "tags" => Config.get(:server_tags),
      "jetstream" => js?,
      # JetStreamEnabled capability bit
      "flags" => if(js?, do: 1, else: 0),
      "seq" => next_seq(),
      "time" => DateTime.to_iso8601(DateTime.utc_now()),
      "natsbeam" => srv.version
    }
    |> Map.reject(fn {_, v} -> v in [nil, ""] or v == [] end)
  end

  defp next_seq do
    key = {__MODULE__, :seq}

    ref =
      case :persistent_term.get(key, nil) do
        nil ->
          ref = :atomics.new(1, signed: false)
          :persistent_term.put(key, ref)
          ref

        ref ->
          ref
      end

    :atomics.add_get(ref, 1, 1)
  end

  defp idz do
    srv = Natsbeam.Server.info()
    Jason.encode!(%{"name" => srv.name, "host" => host(), "id" => srv.id})
  end

  defp cluster_name, do: Natsbeam.JetStream.Cluster.cluster_name()

  # The address clients reach us on (nats-server reports its listen host).
  defp host do
    case Config.get(:listeners) do
      [%{host: h} | _] when is_binary(h) and h not in ["", "0.0.0.0", "::"] -> h
      _ -> hostname()
    end
  end

  defp hostname do
    {:ok, h} = :inet.gethostname()
    to_string(h)
  end

  defp links, do: Natsbeam.Link.list() |> Enum.filter(& &1.connected)
  defp route_links, do: Enum.filter(links(), &(&1.kind == :route))
  defp leaf_links, do: Enum.filter(links(), &(&1.kind == :leaf))

  defp totals do
    clients = Natsbeam.Clients.list() |> Enum.reject(&(&1[:kind] == :local))

    sums =
      Enum.reduce(clients, %{in_msgs: 0, out_msgs: 0, in_bytes: 0, out_bytes: 0}, fn c, acc ->
        Map.merge(acc, c[:stats] || %{}, fn _k, a, b -> a + b end)
      end)

    subs =
      Enum.sum(
        for a <- Natsbeam.Account.list(), s = Natsbeam.Account.stats(a), do: s.subscriptions
      )

    %{clients: clients, sums: sums, subs: subs}
  end

  defp statsz(state) do
    %{clients: clients, sums: sums, subs: subs} = totals()
    srv = Natsbeam.Server.info()

    routes =
      for {l, i} <- Enum.with_index(route_links(), 1) do
        %{
          "rid" => i,
          "name" => l.remote_name,
          "sent" => %{"msgs" => l.stats.out_msgs, "bytes" => 0},
          "received" => %{"msgs" => l.stats.in_msgs, "bytes" => 0},
          "pending" => 0
        }
      end

    %{
      "start" => DateTime.to_iso8601(srv.started_at),
      "mem" => :erlang.memory(:total),
      "cores" => System.schedulers_online(),
      "cpu" => 0.0,
      "connections" => length(clients),
      "total_connections" => :atomics.get(srv.counter, 1),
      "active_accounts" => length(Natsbeam.Account.list()),
      "subscriptions" => subs,
      "sent" => %{"msgs" => sums.out_msgs, "bytes" => sums.out_bytes},
      "received" => %{"msgs" => sums.in_msgs, "bytes" => sums.in_bytes},
      "slow_consumers" => 0,
      "active_servers" => map_size(state.servers) + 1,
      "routes" => routes,
      "gateways" => []
    }
    |> maybe_put("jetstream", js_varz())
  end

  # nats-server's JetStreamVarz (config + stats), when JetStream is on.
  defp js_varz do
    if Natsbeam.JetStream.enabled?() do
      jsz = Natsbeam.JetStream.jsz()

      %{
        "config" => maybe_put(jsz["config"], "domain", Natsbeam.JetStream.domain()),
        "stats" =>
          Map.take(
            jsz,
            ~w(memory storage reserved_memory reserved_storage accounts ha_assets api)
          )
      }
    end
  end

  defp maybe_put(map, _k, nil), do: map
  defp maybe_put(map, k, v), do: Map.put(map, k, v)

  defp varz do
    %{clients: clients, sums: sums, subs: subs} = totals()
    srv = Natsbeam.Server.info()
    cfg = Config.all()
    port = Natsbeam.listeners() |> Map.values() |> List.first() || 0
    now = DateTime.utc_now()
    routes = route_links()

    %{
      "server_id" => srv.id,
      "server_name" => srv.name,
      "version" => cfg.advertised_version,
      "proto" => 1,
      "go" => "beam/otp-#{System.otp_release()}",
      "host" => host(),
      "port" => port,
      "auth_required" => Natsbeam.Auth.required?(),
      "config_load_time" => DateTime.to_iso8601(srv.started_at),
      "tags" => cfg.server_tags,
      "max_connections" => cfg.max_connections,
      "ping_interval" => cfg.ping_interval * 1_000_000,
      "ping_max" => cfg.ping_max_outstanding,
      "max_control_line" => cfg.max_control_line,
      "max_payload" => cfg.max_payload,
      "max_pending" => cfg.max_pending_msgs,
      "auth_timeout" => cfg.auth_timeout / 1000,
      "write_deadline" => cfg.write_timeout * 1_000_000,
      "start" => DateTime.to_iso8601(srv.started_at),
      "now" => DateTime.to_iso8601(now),
      "uptime" => uptime(srv.started_at, now),
      "mem" => :erlang.memory(:total),
      "cores" => System.schedulers_online(),
      "cpu" => 0.0,
      "connections" => length(clients),
      "total_connections" => :atomics.get(srv.counter, 1),
      "routes" => length(routes),
      "remotes" => routes |> Enum.map(& &1.remote_id) |> Enum.uniq() |> length(),
      "leafnodes" => length(leaf_links()),
      "in_msgs" => sums.in_msgs,
      "out_msgs" => sums.out_msgs,
      "in_bytes" => sums.in_bytes,
      "out_bytes" => sums.out_bytes,
      "slow_consumers" => 0,
      "subscriptions" => subs,
      "http_host" => "",
      "http_port" => 0,
      "https_port" => 0,
      "cluster" => %{
        "name" => cluster_name(),
        "addr" => "127.0.0.1",
        "cluster_port" => cfg.cluster_port || 0,
        "urls" => cfg.routes || [],
        "pool_size" => Natsbeam.Link.configured_pool_size()
      },
      "leaf" => %{"port" => cfg.leaf_port || 0},
      "jetstream" => js_varz() || %{},
      "system_account" => cfg.auth.system_account,
      "natsbeam" => srv.version
    }
  end

  defp connz(opts) do
    now = DateTime.utc_now()
    account = opts["acc"] || opts["account"]

    conns =
      for c <- Natsbeam.Clients.list(),
          c[:kind] != :local,
          account in [nil, ""] or c.account == account do
        %{
          "cid" => c.id,
          "kind" => "Client",
          "type" => "nats",
          "uptime" => (c.connected_at && uptime(c.connected_at, now)) || "0s",
          "idle" => "0s",
          "last_activity" => DateTime.to_iso8601(now),
          "rtt" => "0s",
          "pending_bytes" => 0,
          "ip" => c.ip,
          "port" => c.port,
          "start" => c.connected_at && DateTime.to_iso8601(c.connected_at),
          "name" => c.name,
          "lang" => c.lang,
          "version" => c.version,
          "account" => c.account,
          "authorized_user" => c.user,
          "tls" => c.tls,
          "subscriptions" => c.subscriptions,
          "in_msgs" => c.stats.in_msgs,
          "out_msgs" => c.stats.out_msgs,
          "in_bytes" => c.stats.in_bytes,
          "out_bytes" => c.stats.out_bytes
        }
      end

    %{
      "server_id" => Natsbeam.Server.info().id,
      "now" => DateTime.to_iso8601(now),
      "num_connections" => length(conns),
      "total" => length(conns),
      "offset" => 0,
      "limit" => max(length(conns), 1024),
      "connections" => conns
    }
  end

  defp routez do
    srv = Natsbeam.Server.info()

    routes =
      for {l, i} <- Enum.with_index(route_links(), 1) do
        %{
          "rid" => i,
          "remote_id" => l.remote_id,
          "remote_name" => l.remote_name,
          "did_solicit" => l.role == :solicited,
          "ip" => "127.0.0.1",
          "port" => l.peer_port || 0,
          "account" => l.route_account,
          "in_msgs" => l.stats.in_msgs,
          "out_msgs" => l.stats.out_msgs,
          "subscriptions" => l.interest_received,
          "pending_size" => 0
        }
      end

    %{
      "server_id" => srv.id,
      "server_name" => srv.name,
      "now" => DateTime.to_iso8601(DateTime.utc_now()),
      "num_routes" => length(routes),
      "routes" => routes
    }
  end

  defp leafz do
    leafs =
      for l <- leaf_links() do
        %{
          "name" => l.remote_name,
          "is_spoke" => l.role == :solicited,
          "account" => l.account,
          "ip" => "127.0.0.1",
          "port" => l.peer_port || 0,
          "in_msgs" => l.stats.in_msgs,
          "out_msgs" => l.stats.out_msgs,
          "subscriptions" => l.interest_received
        }
      end

    %{
      "server_id" => Natsbeam.Server.info().id,
      "now" => DateTime.to_iso8601(DateTime.utc_now()),
      "leafnodes" => length(leafs),
      "leafs" => leafs
    }
  end

  defp subsz do
    %{subs: subs} = totals()

    %{
      "server_id" => Natsbeam.Server.info().id,
      "now" => DateTime.to_iso8601(DateTime.utc_now()),
      "num_subscriptions" => subs,
      "num_cache" => 0,
      "num_inserts" => 0,
      "num_removes" => 0,
      "num_matches" => 0,
      "cache_hit_rate" => 0.0,
      "max_fanout" => 0,
      "avg_fanout" => 0.0
    }
  end

  defp gatewayz do
    %{
      "server_id" => Natsbeam.Server.info().id,
      "now" => DateTime.to_iso8601(DateTime.utc_now()),
      "name" => cluster_name(),
      "host" => "",
      "port" => 0,
      "outbound_gateways" => %{},
      "inbound_gateways" => %{}
    }
  end

  defp accountz do
    %{
      "server_id" => Natsbeam.Server.info().id,
      "now" => DateTime.to_iso8601(DateTime.utc_now()),
      "accounts" => Natsbeam.Account.list(),
      "system_account" => Config.get([:auth, :system_account])
    }
  end

  defp uptime(from, to) do
    secs = DateTime.diff(to, from)
    {d, r} = {div(secs, 86_400), rem(secs, 86_400)}
    {h, r} = {div(r, 3600), rem(r, 3600)}
    {m, s} = {div(r, 60), rem(r, 60)}
    "#{d}d#{h}h#{m}m#{s}s"
  end
end
