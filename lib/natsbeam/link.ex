defmodule Natsbeam.Link do
  @moduledoc """
  A server-to-server connection to a nats-server (Go) or another natsbeam:
  either a **route** (cluster membership) or a **leafnode** (one account; we
  can be the spoke dialling a hub, or the hub accepting a spoke).

  Interest flows out as `RS+`/`LS+` from the accounts' subscription tables,
  remote interest is registered as `{:link, ...}` / `{:route_link, ...}`
  subscriptions so local publishes reach the peer, and inbound
  `RMSG`/`LMSG`/`HMSG` are published locally without ever being relayed to
  other links.

  ## Route pooling

  Like nats-server (`cluster.pool_size`, default 3), a route to one peer is
  several TCP connections, one `Natsbeam.Link` each:

    * **pool connections** `0..size-1`: an account belongs to slot
      `fnv32a(name) rem size`, and its interest and messages use only that
      connection, with the account name as the first `RS+`/`RS-`/`RMSG`/`HMSG`
      argument;
    * **pinned connections** (`route_account`), one per dedicated account
      (the system account, plus `:pinned_accounts`): they
      carry that account alone and the frames omit the account name.

  When we dial, the slot-0 link (the one `connect/3` starts) opens the other
  slots and the pinned accounts once the peer has answered its first PING;
  when the peer dials, each connection announces its slot (`route_pool_idx`)
  or account (`route_account`) in its INFO. Every established route
  connection registers `{:route_conn, remote_server_id, slot}` so a second
  connection for the same slot is closed as a duplicate. A peer without
  `route_pool_size` (or `route_pool_size: 0`/`-1` here) gets the legacy
  no-pool mode: one connection carrying every account.
  """
  use GenServer, restart: :transient
  require Logger

  alias Natsbeam.{Account, Auth, Config, Server, Subject}
  alias Natsbeam.Protocol.Parser

  @js_denies ["$JS.API.>", "$KV.>", "$OBJ.>"]

  defstruct kind: :route,
            role: :solicited,
            url: nil,
            host: nil,
            port: nil,
            opts: [],
            socket: nil,
            transport: :gen_tcp,
            parser: nil,
            phase: :connecting,
            remote: %{},
            remote_id: nil,
            remote_name: nil,
            account: nil,
            nonce: nil,
            headers?: true,
            perms: nil,
            lds: nil,
            sent: %{},
            remote_subs: %{},
            watched: MapSet.new(),
            pings_out: 0,
            attempt: 0,
            timers: %{},
            stats: %{in_msgs: 0, out_msgs: 0},
            connected_at: nil,
            # route pooling: our slot ({:pool, idx} | {:account, name}), the
            # effective pool size (nil = no-pool), the pinned account, the
            # registry key we hold, accounts the peer has pinned connections for
            # (pool connections leave those out), and the slot holder we wait
            # for when another connection owns our slot.
            slot: nil,
            pool_size: nil,
            route_account: nil,
            registered: nil,
            pinned: %{},
            holder: nil,
            start_siblings?: false

  # ------------------------------------------------------------------- api

  @doc """
  Dial a peer. `kind` is `:route` or `:leaf`. Options: `:account` (leaf only,
  default the default account), `:user`/`:pass`/`:token`, `:credentials`
  (path to a .creds file), `:nkey` (seed), `:tls` (ssl opts) and `:name`.
  """
  def connect(kind, url, opts \\ []) when kind in [:route, :leaf] do
    case DynamicSupervisor.start_child(
           Natsbeam.LinkSupervisor,
           {__MODULE__, {kind, :solicited, url, opts}}
         ) do
      {:error, {:already_started, pid}} -> {:error, {:already_connected, pid}}
      other -> other
    end
  end

  defp solicited_via(kind, url, account),
    do: {:via, Registry, {Natsbeam.Registry, {:link, kind, url, account}}}

  @doc false
  def start_accepted(kind, socket, opts \\ []) do
    DynamicSupervisor.start_child(
      Natsbeam.LinkSupervisor,
      {__MODULE__, {kind, :accepted, socket, opts}}
    )
  end

  def start_link({kind, :solicited, url, opts}) do
    account =
      cond do
        kind == :leaf ->
          to_string(Keyword.get(opts, :account) || Config.get([:auth, :default_account]))

        acc = opts[:route_account] ->
          {:account, acc}

        (opts[:pool_idx] || 0) > 0 ->
          {:pool, opts[:pool_idx]}

        true ->
          :all
      end

    GenServer.start_link(__MODULE__, {kind, :solicited, url, opts},
      name: solicited_via(kind, url, account)
    )
  end

  def start_link({kind, role, target, opts}) do
    GenServer.start_link(__MODULE__, {kind, role, target, opts})
  end

  def status(pid), do: GenServer.call(pid, :status)

  @doc "All links with their status."
  def list do
    for {_, pid, _, _} <- DynamicSupervisor.which_children(Natsbeam.LinkSupervisor),
        is_pid(pid),
        st = safe_status(pid),
        st != nil,
        do: st
  end

  defp safe_status(pid) do
    GenServer.call(pid, :status, 2_000)
  catch
    :exit, _ -> nil
  end

  def disconnect(pid), do: GenServer.stop(pid, :normal)

  @doc """
  The route pool slot of an account for a pool of `size` connections:
  `fnv32a(account) rem size` (nats-server's `computeRoutePoolIdx`).
  """
  def pool_slot(_account, size) when size <= 1, do: 0
  def pool_slot(account, size), do: rem(fnv32a(account), size)

  @doc "32-bit FNV-1a hash."
  def fnv32a(data) when is_binary(data) do
    for <<b <- data>>, reduce: 2_166_136_261 do
      h -> Bitwise.band(Bitwise.bxor(h, b) * 16_777_619, 0xFFFFFFFF)
    end
  end

  @doc "Configured route pool size (`:route_pool_size`); 0 or less means no pooling."
  def configured_pool_size do
    case Config.get(:route_pool_size) do
      n when is_integer(n) -> n
      _ -> 0
    end
  end

  @doc """
  Accounts that get a dedicated (pinned) route connection when we dial:
  the system account under its configured name (`$SYS` by default, `SYS` for
  a Go `system_account: SYS`, the public key in operator mode), which
  nats-server always pins, plus `:pinned_accounts`. Only used with pooling.
  """
  def pinned_accounts do
    configured = Enum.map(Config.get(:pinned_accounts) || [], &to_string/1)

    sys =
      case Config.get([:auth, :system_account]) do
        name when is_binary(name) and name != "" -> [name]
        _ -> []
      end

    Enum.uniq(sys ++ configured)
  end

  # ------------------------------------------------------------- callbacks

  @impl true
  def init({kind, :solicited, url, opts}) do
    uri = URI.parse(url)
    opts = Keyword.merge(opts, userinfo(uri))
    Process.flag(:trap_exit, true)

    state = %__MODULE__{
      kind: kind,
      role: :solicited,
      url: url,
      host: uri.host || "127.0.0.1",
      port: uri.port || if(kind == :route, do: 6222, else: 7422),
      opts: opts,
      account: to_string(Keyword.get(opts, :account) || Config.get([:auth, :default_account])),
      route_account: if(kind == :route, do: opts[:route_account])
    }

    {:ok, state, {:continue, :dial}}
  end

  def init({kind, :accepted, socket, opts}) do
    Process.flag(:trap_exit, true)

    state = %__MODULE__{
      kind: kind,
      role: :accepted,
      socket: socket,
      opts: opts,
      parser: Parser.new(mode: kind, max_payload: Config.get(:max_payload)),
      phase: :await_connect,
      nonce: Server.nonce(),
      connected_at: DateTime.utc_now()
    }

    # The acceptor hands us socket ownership and then sends :socket_ready.
    {:ok, state}
  end

  @impl true
  def handle_continue(:dial, state) do
    case :gen_tcp.connect(
           String.to_charlist(state.host),
           state.port,
           [:binary, active: :once, nodelay: true],
           5_000
         ) do
      {:ok, socket} ->
        Logger.info("natsbeam: #{state.kind} link connected to #{state.host}:#{state.port}")

        {:noreply,
         %{
           state
           | socket: socket,
             transport: :gen_tcp,
             parser:
               Parser.new(
                 mode: state.kind,
                 max_payload: Config.get(:max_payload),
                 route_account: state.route_account
               ),
             phase: :await_info,
             connected_at: DateTime.utc_now()
         }
         |> schedule(:info_timeout, 5_000)}

      {:error, reason} ->
        Logger.warning(
          "natsbeam: #{state.kind} link to #{state.host}:#{state.port} failed: #{inspect(reason)}"
        )

        {:noreply, backoff(state)}
    end
  end

  def handle_continue(:accepted, state) do
    # Accept side sends INFO first for both routes and leafs.
    :inet.setopts(state.socket, active: :once)
    write(state, info_frame(state))
    {:noreply, schedule(state, :auth_timeout, 5_000)}
  end

  @impl true
  def handle_call(:status, _from, state) do
    {:reply,
     %{
       pid: self(),
       kind: state.kind,
       role: state.role,
       url: state.url,
       slot: status_slot(state),
       pool_idx: status_pool_idx(state),
       local_port: socket_port(state, :sockname),
       peer_port: socket_port(state, :peername),
       pool_size: state.pool_size,
       route_account: state.route_account,
       connected: state.phase == :connected,
       phase: state.phase,
       remote_id: state.remote_id,
       remote_name: state.remote_name,
       remote_cluster: state.remote["cluster"],
       # leaf: its account; route: the pinned account, nil for pool/no-pool connections
       account: if(state.kind == :leaf, do: state.account, else: state.route_account),
       interest_sent: map_size(state.sent),
       interest_received: map_size(state.remote_subs),
       stats: state.stats,
       connected_at: state.connected_at
     }, state}
  end

  # Another connection to the same peer won the slot we hold (see claim_slot/1).
  def handle_call({:yield_slot, winner}, _from, %{role: :accepted} = state) do
    Logger.debug(
      "natsbeam: route #{inspect(state.slot)} to #{state.remote_name} yields to #{inspect(winner)}"
    )

    {:stop, :normal, :ok, state}
  end

  def handle_call({:yield_slot, winner}, _from, state) do
    if state.socket, do: state.transport.close(state.socket)
    state = detach(%{state | socket: nil})
    {:reply, :ok, go_dormant(state, winner)}
  end

  defp status_slot(%{kind: :leaf}), do: nil
  defp status_slot(%{slot: nil}), do: nil
  defp status_slot(%{pool_size: nil, route_account: nil}), do: :no_pool
  defp status_slot(%{slot: slot}), do: slot

  defp status_pool_idx(%{slot: {:pool, idx}, pool_size: size}) when is_integer(size), do: idx
  defp status_pool_idx(_), do: nil

  defp socket_port(%{socket: nil}, _), do: nil

  defp socket_port(state, which) do
    mod = if state.transport == :ssl, do: :ssl, else: :inet

    case apply(mod, which, [state.socket]) do
      {:ok, {_ip, port}} -> port
      _ -> nil
    end
  end

  # ------------------------------------------------------------ socket io

  @impl true
  def handle_info(:socket_ready, %{role: :accepted} = state),
    do: {:noreply, state, {:continue, :accepted}}

  def handle_info({tag, _sock, _data}, %{socket: nil} = state) when tag in [:tcp, :ssl],
    do: {:noreply, state}

  def handle_info({tag, _sock, data}, state) when tag in [:tcp, :ssl] do
    setopts(state, active: :once)
    process_data(state, data)
  end

  def handle_info({tag, _sock}, state) when tag in [:tcp_closed, :ssl_closed] do
    Logger.info("natsbeam: #{state.kind} link to #{state.remote_name || state.host} closed")
    close_and_retry(%{state | socket: nil}, :closed)
  end

  def handle_info({tag, _sock, reason}, state) when tag in [:tcp_error, :ssl_error] do
    close_and_retry(%{state | socket: nil}, reason)
  end

  # -- local interest changed in an account we watch
  def handle_info({:interest, account, subject, queue, weight}, %{phase: :connected} = state) do
    # Notifications may still be in flight for an account we stopped watching
    # (moved to a pinned connection): those are not ours to advertise.
    if MapSet.member?(state.watched, account),
      do: {:noreply, sync_interest(state, account, subject, queue, weight)},
      else: {:noreply, state}
  end

  def handle_info({:interest, _, _, _, _}, state), do: {:noreply, state}

  # -- a local publish matched one of the peer's subscriptions: forward it
  def handle_info(
        {:natsbeam_msg, {tag, {acc, _subj, queue}}, subject, reply, headers, payload},
        %{phase: :connected} = state
      )
      when tag in [:link, :route_link] do
    if may_publish?(state, subject) do
      headers = if state.headers?, do: headers, else: nil
      write(state, msg_frame(state, acc, subject, reply, queue, headers, payload))
      {:noreply, bump(state, :out_msgs)}
    else
      {:noreply, state}
    end
  end

  def handle_info({:natsbeam_msg, _, _, _, _, _}, state), do: {:noreply, state}

  # -- new accounts appear: routes watch the ones their slot carries
  def handle_info({:account_started, name}, %{kind: :route, phase: :connected} = state) do
    {:noreply, if(carries?(state, name), do: watch_account(state, name), else: state)}
  end

  def handle_info({:account_started, _}, state), do: {:noreply, state}

  # -- the peer's set of pinned-account connections changed
  def handle_info(:rebalance, %{phase: :connected} = state), do: {:noreply, rebalance(state)}
  def handle_info(:rebalance, state), do: {:noreply, state}

  # -- the slot-0 link we were opened by is going away for good
  def handle_info(:stop_sibling, state), do: {:stop, :normal, state}

  def handle_info({:DOWN, ref, :process, _pid, _}, %{holder: {ref, _}} = state) do
    # The connection that owned our slot is gone: dial again (jittered so both
    # servers do not collide on the same slot every time).
    {:noreply, schedule(%{state | holder: nil}, :redial, 200 + :rand.uniform(800))}
  end

  def handle_info({:DOWN, ref, :process, _pid, _}, state) do
    if Enum.any?(state.pinned, fn {_, {_, r}} -> r == ref end) and state.phase == :connected,
      do: {:noreply, rebalance(state)},
      else: {:noreply, state}
  end

  def handle_info(:stable, %{phase: :connected} = state) do
    Logger.info(
      "natsbeam: #{state.kind} link established with #{state.remote_name} (#{state.role})"
    )

    {:noreply, %{state | attempt: 0}}
  end

  def handle_info(:stable, state), do: {:noreply, state}

  def handle_info(:ping, %{phase: phase} = state) when phase != :connected,
    do: {:noreply, state}

  def handle_info(:ping, state) do
    if state.pings_out >= 2 do
      close_and_retry(state, :stale)
    else
      write(state, "PING\r\n")
      {:noreply, %{state | pings_out: state.pings_out + 1} |> schedule(:ping, 30_000)}
    end
  end

  def handle_info(:info_timeout, %{phase: :await_info} = state),
    do: close_and_retry(state, :no_info)

  def handle_info(:info_timeout, state), do: {:noreply, state}

  def handle_info(:auth_timeout, %{phase: :await_connect} = state),
    do: close_and_retry(state, :auth_timeout)

  def handle_info(:auth_timeout, state), do: {:noreply, state}

  def handle_info(:connect_info_timeout, %{phase: :await_connect_info} = state),
    do: close_and_retry(state, :no_connect_info)

  def handle_info(:connect_info_timeout, state), do: {:noreply, state}
  def handle_info(:redial, %{holder: {_, _}} = state), do: {:noreply, state}
  def handle_info(:redial, state), do: {:noreply, state, {:continue, :dial}}
  def handle_info({:EXIT, _, _}, state), do: {:noreply, state}
  def handle_info(_, state), do: {:noreply, state}

  @impl true
  def terminate(reason, state) do
    detach(state)
    if state.socket, do: state.transport.close(state.socket)

    # A deliberate stop of the slot-0 route also ends the pool connections and
    # pinned-account connections it opened (a crash restarts it instead).
    if primary?(state) and reason in [:normal, :shutdown] do
      for pid <- sibling_pids(state), do: send(pid, :stop_sibling)
    end

    :ok
  end

  defp process_data(state, data) do
    case Parser.parse(state.parser, data) do
      {:ok, ops, parser} ->
        state = %{state | parser: parser}

        case Enum.reduce_while(ops, state, fn op, acc ->
               case handle_op(op, acc) do
                 {:ok, acc} -> {:cont, acc}
                 {:close, reason, acc} -> {:halt, {:close, reason, acc}}
               end
             end) do
          {:close, reason, state} ->
            close_and_retry(state, reason)

          state ->
            # Route parsing pauses after each INFO (it may switch the pinned
            # account of the frames that follow); resume on what is buffered.
            if match?({:info, _}, List.last(ops)) and state.socket != nil and
                 state.parser.buf != "",
               do: process_data(state, ""),
               else: {:noreply, state}
        end

      {:error, reason, _parser} ->
        Logger.warning(
          "natsbeam: #{state.kind} link parser error: #{reason} on #{inspect(binary_part(data, 0, min(byte_size(data), 300)))}"
        )

        close_and_retry(state, :parser_error)
    end
  end

  # ------------------------------------------------------------ handshake

  # Peer INFO (first thing a solicited link sees; on accepted routes it arrives after CONNECT).
  defp handle_op({:info, info}, %{phase: :await_info, role: :solicited} = state) do
    state = cancel(state, :info_timeout)

    state = %{
      state
      | remote: info,
        remote_id: info["server_id"],
        remote_name: info["server_name"] || info["server_id"],
        nonce: info["nonce"]
    }

    with :ok <- check_remote_info(state),
         {:ok, state} <- solicited_route_mode(state, info) do
      state = maybe_tls(state)

      try do
        case state.kind do
          :route ->
            case claim_slot(state) do
              {:ok, state} ->
                write(state, [
                  "CONNECT ",
                  Jason.encode!(route_connect(state)),
                  "\r\n",
                  info_frame(state, solicited_route_info(state))
                ])

                established(state)

              {:dormant, holder} ->
                {:close, {:dormant, holder}, state}

              {:close, reason} ->
                {:close, reason, state}
            end

          :leaf ->
            write(state, ["CONNECT ", Jason.encode!(leaf_connect(state)), "\r\n"])

            if info["info_on_connect"] == true do
              {:ok,
               %{state | phase: :await_connect_info} |> schedule(:connect_info_timeout, 5_000)}
            else
              established(state)
            end
        end
      catch
        {:bad_credentials, path, reason} ->
          {:close, {:fatal, "cannot use credentials #{path}: #{inspect(reason)}"}, state}
      end
    else
      {:error, reason} -> {:close, reason, state}
    end
  end

  # Leaf hub's second INFO carrying connect_info / remote_account / perms.
  defp handle_op({:info, info}, %{phase: :await_connect_info} = state) do
    if info["connect_info"] == true do
      state = cancel(state, :connect_info_timeout)
      state = %{state | remote: Map.merge(state.remote, info), perms: leaf_perms(info)}
      established(state)
    else
      {:ok, %{state | remote: Map.merge(state.remote, info)}}
    end
  end

  # Accepted route: CONNECT then the peer's INFO.
  defp handle_op(
         {:connect, opts},
         %{phase: :await_connect, role: :accepted, kind: :route} = state
       ) do
    state = cancel(state, :auth_timeout)

    cond do
      Map.has_key?(opts, "lang") ->
        {:close, :client_on_route_port, state}

      opts["cluster"] not in [nil, cluster_name()] ->
        write(
          state,
          "-ERR 'Rejecting connection, cluster name \"#{opts["cluster"]}\" does not match \"#{cluster_name()}\"'\r\n"
        )

        {:close, :cluster_mismatch, state}

      not route_authorized?(opts) ->
        write(state, "-ERR 'Authorization Violation'\r\n")
        {:close, :unauthorized, state}

      true ->
        {:ok,
         %{
           state
           | phase: :await_peer_info,
             headers?: opts["headers"] == true,
             remote_id: opts["name"]
         }}
    end
  end

  defp handle_op({:info, info}, %{phase: :await_peer_info, kind: :route} = state) do
    state = %{
      state
      | remote: info,
        remote_id: info["server_id"] || state.remote_id,
        remote_name: info["server_name"] || info["server_id"]
    }

    ours = configured_pool_size()
    theirs = int(info["route_pool_size"])
    pooled? = ours > 0 and theirs > 0
    effective = max(ours, theirs)
    account = if info["route_account"] in [nil, ""], do: nil, else: info["route_account"]
    idx = int(info["route_pool_idx"])

    cond do
      info["server_id"] == Server.info().id ->
        {:close, :duplicate_route, state}

      account != nil and not pooled? ->
        write(
          state,
          "-ERR 'Not possible to have a dedicated route for account \"#{account}\" between those servers'\r\n"
        )

        {:close, :pinned_route, state}

      pooled? and (idx < 0 or idx >= effective) ->
        write(
          state,
          "-ERR 'Invalid route pool index: #{idx} - pool size is #{theirs}'\r\n"
        )

        {:close, :invalid_pool_index, state}

      true ->
        state = %{
          state
          | slot:
              if(account, do: {:account, account}, else: {:pool, if(pooled?, do: idx, else: 0)}),
            pool_size: if(pooled?, do: effective),
            route_account: account,
            parser: Parser.set_route_account(state.parser, account)
        }

        case claim_slot(state) do
          {:ok, state} -> established(state)
          {:dormant, _} -> {:close, :duplicate_route, state}
          {:close, reason} -> {:close, reason, state}
        end
    end
  end

  # Accepted leaf (we are the hub): CONNECT with auth, then second INFO + our interest.
  defp handle_op({:connect, opts}, %{phase: :await_connect, role: :accepted, kind: :leaf} = state) do
    state = cancel(state, :auth_timeout)

    client = %{
      id: 0,
      ip: peer_ip(state),
      tls?: state.transport == :ssl,
      connection_type: "LEAFNODE"
    }

    cond do
      Map.has_key?(opts, "lang") ->
        {:close, :client_on_leaf_port, state}

      opts["cluster"] == cluster_name() ->
        write(
          state,
          "-ERR 'Leafnode Error - remote cluster name \"#{opts["cluster"]}\" matches the local one'\r\n"
        )

        {:close, :same_cluster, state}

      true ->
        case Auth.authenticate(opts, state.nonce, client) do
          {:ok, identity} ->
            state = %{
              state
              | account: identity.account,
                remote: opts,
                remote_id: opts["server_id"],
                remote_name: opts["name"] || opts["server_id"],
                headers?: opts["headers"] == true
            }

            write(
              state,
              info_frame(state, %{
                "connect_info" => true,
                "remote_account" => identity.account,
                # like nats-server: tell the spoke it is bound to our system account
                "acc_is_sys" => identity.account == Config.get([:auth, :system_account]) || nil
              })
            )

            established(state)

          {:error, reason} ->
            write(state, "-ERR '#{reason}'\r\n")
            {:close, :unauthorized, state}
        end
    end
  end

  # Async INFO after connect (gossip, leafnode_urls, ldm): accept and ignore.
  defp handle_op({:info, info}, %{phase: :connected} = state) do
    if info["ldm"] == true,
      do: Logger.info("natsbeam: peer #{state.remote_name} entering lame duck mode")

    {:ok, state}
  end

  defp handle_op({:info, _}, state), do: {:ok, state}
  defp handle_op({:connect, _}, state), do: {:ok, state}

  # ---------------------------------------------------------------- ops

  defp handle_op(:ping, state) do
    write(state, "PONG\r\n")
    {:ok, state}
  end

  # The peer answered our first PING on the slot-0 route: it accepted us, so
  # open the rest of the pool (nats-server does the same on its first PONG).
  defp handle_op(:pong, %{start_siblings?: true} = state) do
    start_siblings(state)
    {:ok, %{state | pings_out: 0, start_siblings?: false}}
  end

  defp handle_op(:pong, state), do: {:ok, %{state | pings_out: 0}}

  defp handle_op({:err, msg}, state) do
    Logger.warning(
      "natsbeam: #{state.kind} link #{state.remote_name || state.host} sent -ERR '#{msg}'"
    )

    cond do
      String.contains?(msg, "Loop detected") -> {:close, {:loop, 30_000}, state}
      String.contains?(msg, "cluster name") -> {:close, {:fatal, msg}, state}
      String.contains?(msg, "Duplicate Remote") -> {:close, {:fatal, msg}, state}
      String.contains?(msg, "dedicated route for account") -> {:close, {:fatal, msg}, state}
      String.contains?(msg, "No route for account") -> {:close, {:fatal, msg}, state}
      state.kind == :route -> {:close, {:err, msg}, state}
      true -> {:ok, state}
    end
  end

  # -- remote interest

  defp handle_op({:rs_plus, acc, subject, queue, weight}, state),
    do: {:ok, add_remote_interest(state, acc, subject, queue, weight)}

  defp handle_op({:rs_minus, acc, subject, queue}, state),
    do: {:ok, drop_remote_interest(state, acc, subject, queue)}

  defp handle_op({:ls_plus, subject, _queue, _weight}, %{lds: lds} = state)
       when subject == lds and lds != nil do
    write(
      state,
      "-ERR 'Loop detected for leafnode account=\"#{state.account}\". Delaying attempt to reconnect for 30s'\r\n"
    )

    {:close, {:loop, 30_000}, state}
  end

  defp handle_op({:ls_plus, subject, queue, weight}, state),
    do: {:ok, add_remote_interest(state, state.account, subject, queue, weight)}

  defp handle_op({:ls_minus, subject, queue}, state),
    do: {:ok, drop_remote_interest(state, state.account, subject, queue)}

  # -- inbound messages

  defp handle_op({:rmsg, acc, subject, reply, queues, headers, payload}, state) do
    {:ok, deliver_in(state, acc, subject, reply, queues, headers, payload)}
  end

  defp handle_op({:lmsg, subject, reply, queues, headers, payload}, state) do
    {:ok, deliver_in(state, state.account, subject, reply, queues, headers, payload)}
  end

  defp handle_op({:unknown, _}, state), do: {:ok, state}
  defp handle_op(_, state), do: {:ok, state}

  # Hub-supplied permissions: `import` limits what we may subscribe to (interest we
  # advertise), `export` limits what we may publish to the hub.
  defp leaf_perms(info) do
    if info["import"] || info["export"] do
      Natsbeam.Permissions.from_map(%{"sub" => info["import"], "pub" => info["export"]})
    end
  end

  defp may_subscribe?(%{perms: nil}, _subject), do: true

  defp may_subscribe?(%{perms: p}, subject),
    do: Natsbeam.Permissions.can_subscribe?(p, subject, nil)

  defp may_publish?(%{perms: nil}, _subject), do: true
  defp may_publish?(%{perms: p}, subject), do: Natsbeam.Permissions.can_publish?(p, subject)

  # ----------------------------------------------------------- established

  defp established(state) do
    # The peer may still reject us (bad credentials arrive as -ERR after our
    # CONNECT), so the link only counts as established once it has stayed up.
    state = schedule(state, :stable, 2_000)

    state = %{state | phase: :connected, pings_out: 0, sent: %{}} |> schedule(:ping, 30_000)

    state =
      case state.kind do
        :route ->
          :telemetry.attach(
            {__MODULE__, self()},
            [:natsbeam, :account, :start],
            &__MODULE__.on_account_start/4,
            self()
          )

          state = refresh_pinned(state)

          state =
            Account.list()
            |> Enum.filter(&carries?(state, &1))
            |> Enum.reduce(state, &watch_account(&2, &1))

          # A new pinned connection takes its account away from the pool connections.
          if match?({:account, _}, state.slot), do: notify_pool(state.remote_id)

          if primary?(state) and state.pool_size != nil do
            write(state, "PING\r\n")
            %{state | start_siblings?: true}
          else
            state
          end

        :leaf ->
          watch_account(state, state.account)
      end

    # Reach the peer's JetStream through its domain without waiting for interest.
    state = implicit_js_interest(state)

    # Loop detection subject, like nats-server's $LDS.<id>: if it ever comes
    # back to us as interest, our own interest has looped around.
    state =
      if state.kind == :leaf do
        lds = "$LDS." <> Server.info().id
        write(state, ["LS+ ", lds, "\r\n"])
        %{state | lds: lds}
      else
        state
      end

    :telemetry.execute([:natsbeam, :link, :up], %{}, %{
      kind: state.kind,
      remote: state.remote_name
    })

    {:ok, state}
  end

  @doc false
  def on_account_start(_event, _m, %{account: name}, pid), do: send(pid, {:account_started, name})

  defp watch_account(state, account) do
    if MapSet.member?(state.watched, account) do
      state
    else
      {:ok, interest} =
        Account.watch_interest(account, self(), exclude_routes: state.kind == :route)

      state = %{state | watched: MapSet.put(state.watched, account)}

      Enum.reduce(interest, state, fn {subject, queue, weight}, acc ->
        sync_interest(acc, account, subject, queue, weight)
      end)
    end
  end

  # Send RS+/LS+ (or RS-/LS-) when the weight from our side changes.
  defp sync_interest(state, account, subject, queue, weight) do
    key = {account, subject, queue}
    prev = Map.get(state.sent, key, 0)

    cond do
      not propagate?(state, account, subject) ->
        state

      weight == prev ->
        state

      weight == 0 ->
        write(state, unsub_frame(state, account, subject, queue))
        %{state | sent: Map.delete(state.sent, key)}

      queue == nil and prev > 0 ->
        %{state | sent: Map.put(state.sent, key, weight)}

      true ->
        write(state, sub_frame(state, account, subject, queue, weight))
        %{state | sent: Map.put(state.sent, key, weight)}
    end
  end

  # Interest we never advertise: our own JetStream API (both servers run one)
  # and, on leafs, the JetStream subject space (nats does the same across domains).
  defp propagate?(%{kind: :leaf} = state, account, subject) do
    account == state.account and subject != "$JS.API.>" and
      not Enum.any?(@js_denies, &Subject.intersects?(&1, subject)) and
      may_subscribe?(state, subject)
  end

  defp propagate?(_state, _account, subject), do: subject != "$JS.API.>"

  defp add_remote_interest(state, account, subject, queue, _weight) do
    key = {account, subject, queue}

    cond do
      Map.has_key?(state.remote_subs, key) ->
        state

      subject == "$JS.API.>" or
          (state.kind == :leaf and Enum.any?(@js_denies, &Subject.intersects?(&1, subject))) ->
        state

      not Subject.valid_subscription?(subject) ->
        state

      true ->
        # Accounts are created on demand here (like nats-server's non-static
        # resolvers): interest may arrive before anything local touched the
        # account, e.g. the default account before any client connected.
        case Account.ensure(account) do
          {:ok, apid, _} ->
            :ok = Account.subscribe(apid, self(), sid(state, key), subject, queue)
            %{state | remote_subs: Map.put(state.remote_subs, key, true)}

          _ ->
            state
        end
    end
  end

  defp drop_remote_interest(state, account, subject, queue) do
    key = {account, subject, queue}

    case Account.lookup(account) do
      {:ok, apid, _} -> Account.unsubscribe(apid, self(), sid(state, key))
      :error -> :ok
    end

    %{state | remote_subs: Map.delete(state.remote_subs, key)}
  end

  # Remote interest is a subscription owned by the link; route ones are tagged
  # so route watchers can leave them out (route interest is never relayed).
  defp sid(%{kind: :route}, key), do: {:route_link, key}
  defp sid(_, key), do: {:link, key}

  # If the peer runs JetStream in a domain, make `$JS.<domain>.API.>` reach it
  # from every local account the link carries, even before it advertises interest.
  defp implicit_js_interest(state) do
    case state.remote["domain"] do
      d when is_binary(d) and d != "" ->
        accounts =
          if state.kind == :leaf,
            do: [state.account],
            else: Enum.filter(Account.list(), &carries?(state, &1))

        Enum.reduce(accounts, state, fn acc, st ->
          add_remote_interest(st, acc, "$JS.#{d}.API.>", nil, 1)
        end)

      _ ->
        state
    end
  end

  defp deliver_in(state, account, subject, reply, queues, headers, payload) do
    # JetStream deliveries: the frame subject is the delivery target (matched
    # here) and `$JS.ACK...@<original>` carries the subject clients should see.
    {match_subject, subject, reply} =
      case reply && js_ack_split(reply) do
        {ack, original} -> {subject, original, ack}
        _ -> {subject, subject, reply}
      end

    case Account.lookup(account) do
      {:ok, _apid, sublist} ->
        Account.publish_from_link(
          sublist,
          match_subject,
          subject,
          reply,
          headers,
          payload,
          if(queues == [], do: nil, else: queues)
        )

        bump(state, :in_msgs)

      :error ->
        state
    end
  end

  # Split a `$JS.ACK.<s>.<c>.<d>.<ss>.<cs>.<t>.<p>@<deliver>` reply at the first '@'
  # after the eight dots of the ACK token (subjects may legally contain '@').
  defp js_ack_split("$JS.ACK." <> _ = reply) do
    find_at(reply, 0, 0)
  end

  defp js_ack_split(_), do: nil

  defp find_at(reply, i, dots) when i < byte_size(reply) do
    case binary_part(reply, i, 1) do
      "." ->
        find_at(reply, i + 1, dots + 1)

      "@" when dots >= 8 ->
        {binary_part(reply, 0, i), binary_part(reply, i + 1, byte_size(reply) - i - 1)}

      _ ->
        find_at(reply, i + 1, dots)
    end
  end

  defp find_at(_, _, _), do: nil

  # ------------------------------------------------------------- frames

  defp info_frame(state, extra \\ %{}) do
    srv = Server.info()
    cfg = Config.all()
    {host, port} = advertised_address(state)

    base = %{
      "server_id" => srv.id,
      "server_name" => srv.name,
      "version" => cfg.advertised_version,
      "proto" => if(state.kind == :route, do: 2, else: 1),
      "go" => "beam",
      "host" => host,
      "port" => port,
      "headers" => true,
      "max_payload" => cfg.max_payload,
      "cluster" => cluster_name(),
      "jetstream" => Natsbeam.JetStream.enabled?(),
      "domain" => Natsbeam.JetStream.domain()
    }

    kind_fields =
      case state.kind do
        :route ->
          ps = configured_pool_size()

          %{
            "gossip_mode" => if(port == 0, do: 1, else: 0),
            "route_pool_size" => if(ps > 0, do: ps)
          }

        :leaf ->
          %{
            "auth_required" => true,
            "nonce" => state.nonce,
            "client_id" => :erlang.phash2(self()) + 1,
            "info_on_connect" => true,
            "leafnode_urls" => [if(port > 0, do: "#{host}:#{port}", else: "#{host}:7422")]
          }
      end

    info =
      base
      |> Map.merge(kind_fields)
      |> Map.merge(extra)
      |> Enum.reject(fn {_, v} -> is_nil(v) or (v == 0 and false) end)
      |> Enum.reject(fn {k, v} -> k == "gossip_mode" and v == 0 end)
      |> Map.new()

    ["INFO ", Jason.encode!(info), "\r\n"]
  end

  # The INFO we send after CONNECT on a route we dialled names our slot.
  defp solicited_route_info(%{route_account: acc}) when is_binary(acc),
    do: %{"route_account" => acc}

  defp solicited_route_info(%{pool_size: size, slot: {:pool, idx}})
       when is_integer(size) and idx > 0,
       do: %{"route_pool_idx" => idx}

  defp solicited_route_info(_), do: %{}

  defp advertised_address(state) do
    port =
      case state.kind do
        :route -> Config.get(:cluster_port)
        :leaf -> Config.get(:leaf_port)
      end

    {"127.0.0.1", port || 0}
  end

  defp route_connect(state) do
    opts = state.opts

    %{
      "echo" => true,
      "verbose" => false,
      "pedantic" => false,
      "tls_required" => false,
      "headers" => true,
      "name" => Server.info().id,
      "cluster" => cluster_name(),
      "cluster_dynamic" => false
    }
    |> maybe_auth(opts, state.nonce)
  end

  defp leaf_connect(state) do
    opts = state.opts

    %{
      "version" => Config.get(:advertised_version),
      "server_id" => Server.info().id,
      "name" => Server.info().name,
      "headers" => true,
      "cluster" => cluster_name(),
      "jetstream" => Natsbeam.JetStream.enabled?(),
      "domain" => Natsbeam.JetStream.domain(),
      "remote_account" => state.account,
      "protocol" => 1
    }
    |> Enum.reject(fn {_, v} -> is_nil(v) end)
    |> Map.new()
    |> maybe_auth(opts, state.nonce)
  end

  defp maybe_auth(connect, opts, nonce) do
    cond do
      creds = Keyword.get(opts, :credentials) ->
        with {:ok, body} <- File.read(creds),
             {:ok, jwt, seed} <- Natsbeam.JWT.parse_creds(body) do
          Map.merge(connect, %{"jwt" => jwt, "sig" => sign(seed, nonce)})
        else
          {:error, reason} -> throw({:bad_credentials, creds, reason})
        end

      seed = Keyword.get(opts, :nkey) ->
        {:ok, pub} = Natsbeam.NKeys.public_from_seed(seed)
        Map.merge(connect, %{"nkey" => pub, "sig" => sign(seed, nonce)})

      token = Keyword.get(opts, :token) ->
        Map.put(connect, "auth_token", token)

      user = Keyword.get(opts, :user) ->
        Map.merge(connect, %{"user" => user, "pass" => Keyword.get(opts, :pass, "")})

      true ->
        connect
    end
  end

  defp sign(_seed, nil), do: nil

  defp sign(seed, nonce) do
    {:ok, sig} = Natsbeam.NKeys.sign(seed, nonce)
    Base.url_encode64(sig, padding: false)
  end

  # Pinned-account route connections leave the account out of every frame.
  defp acc_arg(%{kind: :route, route_account: nil}, account), do: [account, " "]
  defp acc_arg(_state, _account), do: []

  defp sub_frame(%{kind: :route} = s, account, subject, nil, _w),
    do: ["RS+ ", acc_arg(s, account), subject, "\r\n"]

  defp sub_frame(%{kind: :route} = s, account, subject, queue, w),
    do: ["RS+ ", acc_arg(s, account), subject, " ", queue, " ", Integer.to_string(w), "\r\n"]

  defp sub_frame(%{kind: :leaf}, _account, subject, nil, _w), do: ["LS+ ", subject, "\r\n"]

  defp sub_frame(%{kind: :leaf}, _account, subject, queue, w),
    do: ["LS+ ", subject, " ", queue, " ", Integer.to_string(w), "\r\n"]

  defp unsub_frame(%{kind: :route} = s, account, subject, nil),
    do: ["RS- ", acc_arg(s, account), subject, "\r\n"]

  defp unsub_frame(%{kind: :route} = s, account, subject, queue),
    do: ["RS- ", acc_arg(s, account), subject, " ", queue, "\r\n"]

  defp unsub_frame(%{kind: :leaf}, _account, subject, nil), do: ["LS- ", subject, "\r\n"]

  defp unsub_frame(%{kind: :leaf}, _account, subject, queue),
    do: ["LS- ", subject, " ", queue, "\r\n"]

  # RMSG/LMSG (no headers) or HMSG (headers), with the queue-target forms.
  defp msg_frame(state, account, subject, reply, queue, headers, payload) do
    op = if headers, do: "HMSG ", else: if(state.kind == :route, do: "RMSG ", else: "LMSG ")
    acc = acc_arg(state, account)

    targets =
      case {reply, queue} do
        {nil, nil} -> []
        {r, nil} -> [r, " "]
        {nil, q} -> ["| ", q, " "]
        {r, q} -> ["+ ", r, " ", q, " "]
      end

    sizes =
      if headers do
        [
          Integer.to_string(byte_size(headers)),
          " ",
          Integer.to_string(byte_size(headers) + byte_size(payload))
        ]
      else
        Integer.to_string(byte_size(payload))
      end

    [op, acc, subject, " ", targets, sizes, "\r\n", headers || "", payload, "\r\n"]
  end

  # ------------------------------------------------------------- pooling

  # Our side of nats-server's addRoute() for a route we dialled: pooled when
  # both servers have a pool size, else the legacy single connection.
  defp solicited_route_mode(%{kind: :leaf} = state, _info), do: {:ok, state}

  defp solicited_route_mode(state, info) do
    ours = configured_pool_size()
    theirs = int(info["route_pool_size"])
    pooled? = ours > 0 and theirs > 0
    effective = max(ours, theirs)
    idx = state.opts[:pool_idx] || 0

    cond do
      state.route_account != nil and not pooled? ->
        {:error,
         {:fatal,
          "no dedicated route for account #{state.route_account}: route pooling is off on one side"}}

      idx > 0 and not pooled? ->
        {:error, {:stop, "the peer runs without route pooling"}}

      pooled? and int(info["route_pool_idx"]) != 0 ->
        msg = "Route pool index should not be set but is set to #{info["route_pool_idx"]}"
        write(state, "-ERR '#{msg}'\r\n")
        {:error, {:err, msg}}

      pooled? and idx >= effective ->
        {:error, {:stop, "the pool has #{effective} connections"}}

      true ->
        {:ok,
         %{
           state
           | pool_size: if(pooled?, do: effective),
             slot:
               if(state.route_account,
                 do: {:account, state.route_account},
                 else: {:pool, if(pooled?, do: idx, else: 0)}
               )
         }}
    end
  end

  # Register this connection as the one serving its slot with the peer. When
  # another connection already holds it, keep the one the peer kept: a Go
  # server commits to a connection it dialled before telling us its pool index,
  # so the inbound one wins; between two natsbeams the connection dialled by
  # the smaller server id wins. The loser either waits (dialled) or closes.
  defp claim_slot(state, retries \\ 1) do
    key = {:route_conn, state.remote_id, state.slot}

    case Registry.register(Natsbeam.Registry, key, state.role) do
      {:ok, _} ->
        {:ok, %{state | registered: key}}

      {:error, {:already_registered, pid}} when pid == self() ->
        {:ok, %{state | registered: key}}

      {:error, {:already_registered, other}} ->
        other_role =
          case Registry.lookup(Natsbeam.Registry, key) do
            [{^other, role}] -> role
            _ -> nil
          end

        cond do
          other_role == nil and retries > 0 ->
            claim_slot(state, retries - 1)

          other_role != nil and retries > 0 and keep_mine?(state, other_role) ->
            try do
              GenServer.call(other, {:yield_slot, self()}, 5_000)
            catch
              :exit, _ -> :ok
            end

            claim_slot(state, retries - 1)

          state.role == :solicited ->
            {:dormant, other}

          true ->
            {:close, :duplicate_route}
        end
    end
  end

  defp keep_mine?(%{role: role}, role), do: false

  defp keep_mine?(state, _other_role) do
    if state.remote["go"] == "beam" do
      ours = Server.info().id
      if state.role == :solicited, do: ours < state.remote_id, else: state.remote_id < ours
    else
      state.role == :accepted
    end
  end

  defp release_slot(%{registered: nil} = state), do: state

  defp release_slot(%{registered: key} = state) do
    Registry.unregister(Natsbeam.Registry, key)
    if match?({:account, _}, state.slot), do: notify_pool(state.remote_id)
    %{state | registered: nil}
  end

  # Another connection serves our slot: stay idle until it goes away.
  defp go_dormant(state, holder) do
    Logger.info(
      "natsbeam: route #{inspect(state.slot)} to #{state.remote_name} is served by another connection"
    )

    ref = Process.monitor(holder)

    %{state | phase: :dormant, holder: {ref, holder}, socket: nil, attempt: 0}
    |> cancel(:redial)
    |> cancel(:info_timeout)
  end

  # Does this connection carry `account`'s interest and messages?
  defp carries?(%{kind: :leaf} = state, account), do: account == state.account
  defp carries?(%{route_account: acc}, account) when is_binary(acc), do: account == acc
  defp carries?(%{pool_size: nil}, _account), do: true

  defp carries?(%{slot: {:pool, idx}} = state, account) do
    not Map.has_key?(state.pinned, account) and
      pool_slot(account, configured_pool_size()) == idx
  end

  defp carries?(_state, _account), do: false

  # Accounts the peer has a pinned connection for (only pool connections care).
  defp refresh_pinned(%{pool_size: size, slot: {:pool, _}, remote_id: rid} = state)
       when is_integer(size) do
    for {_, {_, ref}} <- state.pinned, do: Process.demonitor(ref, [:flush])

    pinned =
      Registry.select(Natsbeam.Registry, [
        {{{:route_conn, :"$1", {:account, :"$2"}}, :"$3", :_}, [{:==, :"$1", rid}],
         [{{:"$2", :"$3"}}]}
      ])
      |> Map.new(fn {acc, pid} -> {acc, {pid, Process.monitor(pid)}} end)

    %{state | pinned: pinned}
  end

  defp refresh_pinned(state), do: state

  # Re-split accounts after the peer's pinned connections changed: drop the
  # ones this connection no longer carries (withdrawing our interest and the
  # peer's), pick up the ones it now does.
  defp rebalance(state) do
    state = refresh_pinned(state)

    state =
      Enum.reduce(state.watched, state, fn acc, st ->
        if carries?(st, acc), do: st, else: unwatch_account(st, acc)
      end)

    Account.list()
    |> Enum.filter(&carries?(state, &1))
    |> Enum.reduce(state, &watch_account(&2, &1))
  end

  defp unwatch_account(state, account) do
    Account.unwatch_interest(account, self())

    sent =
      Enum.reduce(state.sent, state.sent, fn
        {{^account, subject, queue} = key, _}, acc ->
          write(state, unsub_frame(state, account, subject, queue))
          Map.delete(acc, key)

        _, acc ->
          acc
      end)

    state = %{state | sent: sent, watched: MapSet.delete(state.watched, account)}

    state.remote_subs
    |> Map.keys()
    |> Enum.filter(&match?({^account, _, _}, &1))
    |> Enum.reduce(state, fn {a, s, q}, st -> drop_remote_interest(st, a, s, q) end)
  end

  defp notify_pool(nil), do: :ok

  defp notify_pool(rid) do
    for pid <-
          Registry.select(Natsbeam.Registry, [
            {{{:route_conn, :"$1", {:pool, :_}}, :"$2", :_}, [{:==, :"$1", rid}], [:"$2"]}
          ]),
        pid != self(),
        do: send(pid, :rebalance)

    :ok
  end

  # The route connect/3 started (slot 0 of a URL we dial) owns the others.
  defp primary?(%{kind: :route, role: :solicited, route_account: nil, opts: opts}),
    do: (opts[:pool_idx] || 0) == 0

  defp primary?(_), do: false

  defp start_siblings(state) do
    base = Keyword.drop(state.opts, [:pool_idx, :route_account])

    specs =
      for(k <- 1..(state.pool_size - 1)//1, do: [pool_idx: k]) ++
        for acc <- pinned_accounts(), do: [route_account: acc]

    for extra <- specs do
      DynamicSupervisor.start_child(
        Natsbeam.LinkSupervisor,
        {__MODULE__, {:route, :solicited, state.url, base ++ extra}}
      )
    end

    :ok
  end

  defp sibling_pids(state) do
    url = state.url

    Registry.select(Natsbeam.Registry, [
      {{{:link, :route, :"$1", :"$2"}, :"$3", :_}, [{:==, :"$1", url}, {:"=/=", :"$2", :all}],
       [:"$3"]}
    ])
  end

  defp int(n) when is_integer(n), do: n
  defp int(_), do: 0

  # ------------------------------------------------------------- helpers

  defp check_remote_info(%{kind: :route} = state) do
    cond do
      state.remote["server_id"] == Server.info().id ->
        {:error, :duplicate_route}

      state.remote["cluster"] not in [nil, cluster_name()] and
          state.remote["cluster_dynamic"] != true ->
        {:error,
         {:fatal,
          "cluster name mismatch: peer is #{state.remote["cluster"]}, we are #{cluster_name()}"}}

      true ->
        :ok
    end
  end

  defp check_remote_info(%{kind: :leaf} = state) do
    cond do
      (state.remote["client_id"] || 0) == 0 and state.remote["leafnode_urls"] in [nil, []] and
          state.remote["info_on_connect"] != true ->
        {:error, {:fatal, "peer does not look like a leafnode port"}}

      state.remote["cluster"] == cluster_name() ->
        {:error, {:fatal, "hub cluster name equals ours (#{cluster_name()})"}}

      true ->
        :ok
    end
  end

  defp maybe_tls(%{remote: %{"tls_required" => true}, transport: :gen_tcp} = state) do
    opts = Keyword.get(state.opts, :tls, []) |> Keyword.put_new(:verify, :verify_none)

    case :ssl.connect(state.socket, opts, 5_000) do
      {:ok, ssl} ->
        :ssl.setopts(ssl, active: :once)
        %{state | socket: ssl, transport: :ssl}

      {:error, reason} ->
        Logger.warning("natsbeam: link TLS upgrade failed: #{inspect(reason)}")
        state
    end
  end

  defp maybe_tls(state), do: state

  defp route_authorized?(opts) do
    case Config.get(:cluster_auth) do
      nil -> true
      %{user: u, pass: p} -> opts["user"] == u and Auth.secret_match?(p, opts["pass"] || "")
      [user: u, pass: p] -> opts["user"] == u and Auth.secret_match?(p, opts["pass"] || "")
      _ -> true
    end
  end

  defp cluster_name, do: Natsbeam.JetStream.Cluster.cluster_name()

  defp userinfo(%URI{userinfo: nil}), do: []

  defp userinfo(%URI{userinfo: ui}) do
    case String.split(ui, ":", parts: 2) do
      [user, pass] -> [user: user, pass: pass]
      [token] -> [token: token]
    end
  end

  defp peer_ip(state) do
    case :inet.peername(state.socket) do
      {:ok, {ip, _}} -> to_string(:inet.ntoa(ip))
      _ -> nil
    end
  end

  defp write(%{socket: nil}, _), do: :ok

  defp write(state, iodata) do
    case state.transport.send(state.socket, iodata) do
      :ok -> :ok
      {:error, _} -> :ok
    end
  end

  defp setopts(%{transport: :ssl} = state, opts), do: :ssl.setopts(state.socket, opts)
  defp setopts(state, opts), do: :inet.setopts(state.socket, opts)

  # Drop every subscription we registered on the peer's behalf and stop watching.
  defp detach(state) do
    for {{account, _, _} = key, _} <- state.remote_subs do
      case Account.lookup(account) do
        {:ok, apid, _} -> Account.unsubscribe(apid, self(), sid(state, key))
        :error -> :ok
      end
    end

    for account <- state.watched, do: Account.unwatch_interest(account, self())
    :telemetry.detach({__MODULE__, self()})
    for {_, {_, ref}} <- state.pinned, do: Process.demonitor(ref, [:flush])
    state = release_slot(state)

    %{
      state
      | remote_subs: %{},
        sent: %{},
        watched: MapSet.new(),
        pinned: %{},
        start_siblings?: false
    }
    |> cancel(:ping)
    |> cancel(:stable)
  end

  defp close_and_retry(state, reason) do
    if state.socket, do: state.transport.close(state.socket)
    state = detach(%{state | socket: nil, phase: :connecting})

    :telemetry.execute([:natsbeam, :link, :down], %{}, %{
      kind: state.kind,
      remote: state.remote_name,
      reason: reason
    })

    case {state.role, reason} do
      {:accepted, _} ->
        {:stop, :normal, state}

      {:solicited, {:dormant, holder}} ->
        {:noreply, go_dormant(state, holder)}

      {:solicited, {:stop, why}} ->
        Logger.info(
          "natsbeam: route link to #{state.url} #{inspect(state.slot)} not needed: #{why}"
        )

        {:stop, :normal, state}

      {:solicited, {:fatal, msg}} ->
        Logger.error("natsbeam: #{state.kind} link to #{state.url} refused: #{msg}")
        {:stop, :normal, state}

      {:solicited, {:loop, wait}} ->
        {:noreply, schedule(%{state | attempt: 0}, :redial, wait)}

      {:solicited, _} ->
        {:noreply, backoff(state)}
    end
  end

  defp backoff(state) do
    wait = min(1_000 * Integer.pow(2, min(state.attempt, 5)), 30_000)
    schedule(%{state | attempt: state.attempt + 1}, :redial, wait)
  end

  defp schedule(state, key, ms) do
    if t = state.timers[key], do: Process.cancel_timer(t)
    %{state | timers: Map.put(state.timers, key, Process.send_after(self(), key, ms))}
  end

  defp cancel(state, key) do
    if t = state.timers[key], do: Process.cancel_timer(t)
    %{state | timers: Map.delete(state.timers, key)}
  end

  defp bump(state, key), do: %{state | stats: Map.update!(state.stats, key, &(&1 + 1))}
end
