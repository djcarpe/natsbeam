defmodule Natsbeam.Gateway do
  @moduledoc """
  The nats-server **gateway** protocol: connecting natsbeam (as one cluster
  of a super-cluster) to other clusters.

  Our gateway name is our cluster name (`Natsbeam.JetStream.Cluster.cluster_name/0`).
  One process per TCP connection, in one of two roles. What follows was
  confirmed against nats-server v2.15.1 `server/gateway.go`, `client.go`,
  `parser.go` and `server.go`:

  ## Two connections per pair

  Every pair of gateways runs two independent connections, each used in one
  direction only (`createGateway`, `processGatewayInfo`):

    * **outbound** (we dialled): we send messages (`RMSG`/`HMSG`) and receive
      interest (`RS+`/`RS-`/`A+`/`A-` and `INFO` commands). nats-server closes
      the connection with a protocol violation on `RS+`/`RS-` received on an
      inbound connection, so roles must never be mixed.
    * **inbound** (the remote dialled us): we receive messages and send our
      interest.

  Handshake: the accepting side sends `INFO` first (`gateway` = its name,
  `gateway_urls`, `gateway_url`, `gateway_nrp: true`, `gateway_iom: true`
  since v2.9, `headers`). The dialler checks that `gateway` matches the name
  it was configured with (empty means "wrong port"), then sends `CONNECT`
  (`{"gateway": <own name>, "name": <server id>, "user"/"pass",
  "tls_required", "verbose": false, "pedantic": false}`) followed by its own
  `INFO`. On that first inbound `INFO` nats-server registers the inbound,
  sends `RS+ <acc> <subj> <queue> 1` for all its queue subscriptions, dials
  back the remote's `gateway_urls` if it has no outbound to that name yet
  (an *implicit* gateway, `processImplicitGateway`), gossips its other known
  gateways (`INFO` with `gateway_cmd: 1`), and switches every account to
  interest-only mode. So a Go cluster that is not configured with our URL
  still connects back to us, using the `gateway_urls` from our `INFO`.

  ## Interest

  Per account and per outbound connection the sender keeps one of
  (`outsie`, `gatewayInterest`):

    * no entry: *optimistic*: send everything, unless the peer's `INFO`
      had `gateway_iom: true`, in which case send nothing until told;
    * `A-` received: no interest in the account at all (`A+` clears it);
    * an entry with a no-interest set: `RS- <acc> <subj>` adds the literal
      subject to it, `RS+` removes it (a wildcard `RS+` removes every subject
      it matches); queue subscriptions (`RS+ acc subj q w`) are always kept
      as real subscriptions;
    * after `INFO {"gateway_cmd": 2, "gateway_cmd_payload": base64(acc)}`
      (all-subs start) `RS+`/`RS-` become real subscriptions, and after
      `gateway_cmd: 3` (complete) the no-interest set is dropped:
      **interest-only** mode, a message is sent only on a matching `RS+`.

  The receiving side (`processInboundGatewayMsg`) answers a message for an
  unknown account (or an account without any subscription) with `A-`, and
  a message on a subject without plain subscribers with `RS- <acc> <subj>`
  (once per subject). After 1000 such `RS-` for an account it switches the
  account to interest-only: `INFO` cmd 2, `RS+` for every plain
  subscription, `INFO` cmd 3; afterwards every first/last subscription is
  sent as `RS+`/`RS-`. Since v2.9 nats-server does this switch for every
  account right after accepting a gateway (and advertises `gateway_iom`).
  natsbeam does the same by default (`gateway_interest_only: true`); set it
  to `false` to stay optimistic and answer with `A-`/`RS-`.

  ## Messages

  `RMSG <acc> <subj> [<reply> | + <reply> <q1> <q2>.. | | <q1>..] <size>`
  (`HMSG ... <hdr> <total>` with headers). The receiver delivers to every
  plain subscriber and only to the queue groups listed; a sender lists the
  queue groups that matched the peer's queue interest and that were not
  served in its own cluster. Hence exactly one frame per message per gateway.

  ## Replies across gateways

  A reply subject that matches a subscription made within the last 2 s
  (`recSubExp`) is sent as `_GR_.<cluster hash>.<server hash>.<reply>`,
  hashes being 6 characters of base62 over SHA-256 of the gateway name and
  the server id (`getGWHash`). The receiver strips the prefix before
  delivering and remembers the mapping for a few seconds; when a local
  client publishes on the stripped subject it is sent to the *originating*
  cluster only, with the routed subject, regardless of interest. A message
  whose subject starts with `_GR_.` and the receiver's cluster hash is
  delivered locally under the stripped subject (to every queue group), one
  for another cluster is dropped. `$GR.` is the pre-2.1.2 form: no longer
  sent or handled, only reserved. natsbeam implements both directions (the
  mapping is a short-lived gateway subscription on the stripped reply).

  ## $SYS and $JS

  gateway.go has no special case for `$SYS`: system-account traffic crosses
  only if both sides name the account the same and have interest. JetStream
  in a super-cluster is one Raft meta group that natsbeam does not speak, so
  natsbeam never advertises `$JS.API.>` and never delivers `$JS.API.*`
  messages that arrive over a gateway (both sides run their own JetStream);
  `$JS.<domain>.API.>` interest crosses, so distinct domains reach each other.

  natsbeam never relays: messages received from a gateway go to local
  subscribers and BEAM peers only, and only local (client/in-VM) interest is
  advertised to gateways. Gateway interest is in turn never advertised over
  routes or leafnodes.
  """
  use GenServer, restart: :transient
  require Logger

  alias Natsbeam.{Account, Config, Server, Sublist, Subject}
  alias Natsbeam.Protocol.Parser

  @digits "0123456789ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz"
  @recent_sub_ms 2_000
  @reply_map_ttl 5_000
  @ping_interval 15_000
  @cmd_gossip 1
  @cmd_all_subs_start 2
  @cmd_all_subs_complete 3

  defstruct role: :outbound,
            name: nil,
            url: nil,
            urls: [],
            implicit?: false,
            opts: [],
            socket: nil,
            transport: :gen_tcp,
            parser: nil,
            phase: :connecting,
            remote: %{},
            remote_id: nil,
            remote_name: nil,
            headers?: true,
            iom?: false,
            nrp?: false,
            # outbound: per-account interest (`outsie`) and what is registered in the accounts
            out: %{},
            reg: %{},
            recent: %{},
            # inbound: per-account mode (`insie`), RS+ sent, reply mappings
            insim: %{},
            sent: %{},
            replies: %{},
            watched: MapSet.new(),
            pings_out: 0,
            attempt: 0,
            timers: %{},
            stats: %{in_msgs: 0, out_msgs: 0, rs_plus: 0, rs_minus: 0, a_plus: 0, a_minus: 0},
            connected_at: nil

  # ------------------------------------------------------------------- api

  @doc "Our gateway name: the cluster name."
  def name, do: Natsbeam.JetStream.Cluster.cluster_name()

  @doc "6-character gateway hash of a name or server id (nats-server `getGWHash`)."
  def hash(name) do
    <<b::binary-size(6), _::binary>> = :crypto.hash(:sha256, name)
    for <<x <- b>>, into: "", do: binary_part(@digits, rem(x, 62), 1)
  end

  @doc "The `_GR_.<cluster>.<server>.` prefix for replies routed back to this server."
  def reply_prefix, do: "_GR_." <> hash(name()) <> "." <> hash(Server.info().id) <> "."

  @doc """
  Dial a remote gateway: `connect("nats://host:7222", name: "remote")`.
  `:name` is the remote cluster's gateway name and is required (nats-server
  refuses a gateway whose name does not match). Options: `:user`/`:pass`
  (also from the URL), `:tls` (ssl options, used if the peer requires TLS).
  """
  def connect(url, opts) do
    name = to_string(Keyword.fetch!(opts, :name))

    cond do
      name == name() ->
        {:error, :own_gateway_name}

      true ->
        case DynamicSupervisor.start_child(
               Natsbeam.LinkSupervisor,
               {__MODULE__, {:outbound, url, Keyword.put(opts, :name, name)}}
             ) do
          {:error, {:already_started, pid}} -> {:error, {:already_connected, pid}}
          other -> other
        end
    end
  end

  @doc false
  def start_accepted(socket) do
    DynamicSupervisor.start_child(Natsbeam.LinkSupervisor, {__MODULE__, {:inbound, socket, []}})
  end

  def start_link({:outbound, url, opts}) do
    GenServer.start_link(__MODULE__, {:outbound, url, opts},
      name: {:via, Registry, {Natsbeam.Registry, {:gateway_out, opts[:name]}}}
    )
  end

  def start_link({:inbound, socket, opts}),
    do: GenServer.start_link(__MODULE__, {:inbound, socket, opts})

  def status(pid), do: GenServer.call(pid, :status)

  @doc "Status of every gateway connection (both roles)."
  def list do
    for {_, pid, _, _} <- DynamicSupervisor.which_children(Natsbeam.LinkSupervisor),
        is_pid(pid),
        st = safe_status(pid),
        st != nil,
        st.kind == :gateway,
        do: st
  end

  defp safe_status(pid) do
    GenServer.call(pid, :status, 2_000)
  catch
    :exit, _ -> nil
  end

  def disconnect(pid), do: GenServer.stop(pid, :normal)

  @doc "Close every connection (both directions) with the gateway `name`."
  def remove(name) do
    name = to_string(name)

    for %{pid: pid, gateway: ^name} <- list() do
      try do
        disconnect(pid)
      catch
        :exit, _ -> :ok
      end
    end

    :ok
  end

  @doc false
  def outbound_pid(name) do
    case Registry.lookup(Natsbeam.Registry, {:gateway_out, name}) do
      [{pid, _}] -> pid
      [] -> nil
    end
  end

  defp outbound_by_hash(h) do
    Registry.select(Natsbeam.Registry, [
      {{{:gateway_out, :"$1"}, :"$2", :_}, [], [{{:"$1", :"$2"}}]}
    ])
    |> Enum.find_value(fn {n, pid} -> if hash(n) == h, do: pid end)
  end

  defp inbound_from?(name) do
    Registry.select(Natsbeam.Registry, [{{{:gateway_in, name, :_}, :_, :_}, [], [true]}]) != []
  end

  # ------------------------------------------------------------- callbacks

  @impl true
  def init({:outbound, url, opts}) do
    Process.flag(:trap_exit, true)
    uri = URI.parse(url)
    opts = Keyword.merge(opts, userinfo(uri))
    # The registry value carries the URL we gossip to other gateways.
    Registry.update_value(Natsbeam.Registry, {:gateway_out, opts[:name]}, fn _ ->
      host_port_string(uri)
    end)

    {:ok,
     %__MODULE__{
       role: :outbound,
       name: opts[:name],
       url: url,
       urls: [host_port(uri)],
       implicit?: Keyword.get(opts, :implicit, false),
       opts: opts
     }, {:continue, :dial}}
  end

  def init({:inbound, socket, opts}) do
    Process.flag(:trap_exit, true)

    {:ok,
     %__MODULE__{
       role: :inbound,
       socket: socket,
       opts: opts,
       parser: Parser.new(mode: :gateway, max_payload: Config.get(:max_payload)),
       phase: :await_connect,
       connected_at: DateTime.utc_now()
     }}
  end

  @impl true
  def handle_continue(:dial, state) do
    {host, port} = Enum.at(state.urls, rem(state.attempt, max(length(state.urls), 1)))

    case :gen_tcp.connect(
           String.to_charlist(host),
           port,
           [:binary, active: :once, nodelay: true],
           5_000
         ) do
      {:ok, socket} ->
        Logger.info("natsbeam: gateway connection to #{inspect(state.name)} at #{host}:#{port}")

        {:noreply,
         %{
           state
           | socket: socket,
             transport: :gen_tcp,
             parser: Parser.new(mode: :gateway, max_payload: Config.get(:max_payload)),
             phase: :await_info,
             connected_at: DateTime.utc_now()
         }
         |> schedule(:info_timeout, 5_000)}

      {:error, reason} ->
        Logger.debug(
          "natsbeam: gateway #{inspect(state.name)} at #{host}:#{port} failed: #{inspect(reason)}"
        )

        retry(state)
    end
  end

  def handle_continue(:accepted, state) do
    :inet.setopts(state.socket, active: :once)
    write(state, info_frame())
    {:noreply, schedule(state, :auth_timeout, 5_000)}
  end

  @impl true
  def handle_call(:status, _from, state) do
    {:reply,
     %{
       pid: self(),
       kind: :gateway,
       role: state.role,
       gateway: state.name,
       implicit: state.implicit?,
       url: state.url,
       connected: state.phase == :connected,
       phase: state.phase,
       remote_id: state.remote_id,
       remote_name: state.remote_name,
       remote_cluster: state.name,
       account: nil,
       interest_only: state.iom?,
       modes: modes(state),
       interest_sent: map_size(state.sent),
       interest_received: Enum.sum(for {_, %{sl: sl}} <- state.out, do: MapSet.size(sl)),
       stats: state.stats,
       connected_at: state.connected_at
     }, state}
  end

  defp modes(%{role: :outbound} = state) do
    Map.new(state.out, fn
      {acc, :none} -> {acc, :no_interest}
      {acc, %{mode: m}} -> {acc, m}
    end)
  end

  defp modes(state) do
    Map.new(state.insim, fn
      {acc, :a_minus} -> {acc, :no_interest}
      {acc, %{mode: m}} -> {acc, m}
    end)
  end

  # ------------------------------------------------------------ socket io

  @impl true
  def handle_info(:socket_ready, %{role: :inbound} = state),
    do: {:noreply, state, {:continue, :accepted}}

  def handle_info({tag, _sock, _data}, %{socket: nil} = state) when tag in [:tcp, :ssl],
    do: {:noreply, state}

  def handle_info({tag, _sock, data}, state) when tag in [:tcp, :ssl] do
    setopts(state, active: :once)

    case Parser.parse(state.parser, data) do
      {:ok, ops, parser} ->
        result =
          Enum.reduce_while(ops, %{state | parser: parser}, fn op, acc ->
            case handle_op(op, acc) do
              {:ok, acc} -> {:cont, acc}
              {:close, reason, acc} -> {:halt, {:close, reason, acc}}
            end
          end)

        case result do
          {:close, reason, state} -> close(state, reason)
          state -> {:noreply, state}
        end

      {:error, reason, _} ->
        Logger.warning(
          "natsbeam: gateway #{inspect(state.name)} parser error: #{reason} on " <>
            inspect(binary_part(data, 0, min(byte_size(data), 200)))
        )

        close(state, :parser_error)
    end
  end

  def handle_info({tag, _sock}, state) when tag in [:tcp_closed, :ssl_closed] do
    Logger.info("natsbeam: gateway #{state.role} connection with #{inspect(state.name)} closed")
    close(%{state | socket: nil}, :closed)
  end

  def handle_info({tag, _sock, reason}, state) when tag in [:tcp_error, :ssl_error],
    do: close(%{state | socket: nil}, reason)

  # -- outbound: a local publish matched our subscriptions in an account
  def handle_info(
        {:natsbeam_msg, {:link, {:gw_batch, keys}}, subject, reply, headers, payload},
        %{role: :outbound, phase: :connected} = state
      ) do
    {:noreply, send_message(state, keys, subject, reply, headers, payload)}
  end

  def handle_info({:natsbeam_msg, _, _, _, _, _}, state), do: {:noreply, state}

  # -- local interest changed
  def handle_info({:local_interest, acc, subject, queue, weight}, %{phase: :connected} = state) do
    case state.role do
      :inbound -> {:noreply, inbound_interest(state, acc, subject, queue, weight)}
      :outbound -> {:noreply, note_recent(state, acc, subject, queue, weight)}
    end
  end

  def handle_info({:local_interest, _, _, _, _}, state), do: {:noreply, state}

  def handle_info({:account_started, acc}, %{phase: :connected} = state),
    do: {:noreply, watch_account(state, acc, true)}

  def handle_info({:account_started, _}, state), do: {:noreply, state}

  def handle_info({:expire_reply, key}, state) do
    case Map.pop(state.replies, key) do
      {{apid, opid, _t}, replies} ->
        {acc, routed} = key

        try do
          Account.unsubscribe(apid, opid, {:link, {:gw, acc, {:reply, routed}}})
        catch
          :exit, _ -> :ok
        end

        {:noreply, %{state | replies: replies}}

      {nil, _} ->
        {:noreply, state}
    end
  end

  def handle_info(:ping, state) do
    if state.pings_out >= 3 do
      close(state, :stale)
    else
      write(state, "PING\r\n")
      {:noreply, %{state | pings_out: state.pings_out + 1} |> schedule(:ping, @ping_interval)}
    end
  end

  def handle_info(:info_timeout, %{phase: :await_info} = state), do: close(state, :no_info)

  def handle_info(:auth_timeout, %{phase: phase} = state)
      when phase in [:await_connect, :await_peer_info],
      do: close(state, :auth_timeout)

  def handle_info(:stable, %{phase: :connected} = state), do: {:noreply, %{state | attempt: 0}}
  def handle_info(:redial, state), do: {:noreply, state, {:continue, :dial}}
  def handle_info(_, state), do: {:noreply, state}

  @impl true
  def terminate(_reason, state) do
    detach(state)
    if state.socket, do: state.transport.close(state.socket)
    :ok
  end

  # ------------------------------------------------------------ handshake

  # Outbound: the remote's INFO comes first.
  defp handle_op({:info, info}, %{role: :outbound, phase: :await_info} = state) do
    state = cancel(state, :info_timeout)

    cond do
      info["gateway"] in [nil, ""] ->
        write(
          state,
          "-ERR 'Attempt to connect to gateway #{inspect(state.name)} using wrong port'\r\n"
        )

        {:close, {:fatal, "#{state.url} is not a gateway port"}, state}

      info["gateway"] != state.name ->
        write(
          state,
          "-ERR 'Connection from \"#{name()}\" rejected, wanted to connect to \"#{state.name}\", this is \"#{info["gateway"]}\"'\r\n"
        )

        {:close, {:fatal, "remote gateway is #{inspect(info["gateway"])}"}, state}

      info["server_id"] == Server.info().id ->
        {:close, {:fatal, "connected to ourselves"}, state}

      true ->
        state =
          %{
            state
            | remote: info,
              remote_id: info["server_id"],
              remote_name: info["server_name"] || info["server_id"],
              headers?: info["headers"] == true,
              iom?: info["gateway_iom"] == true,
              nrp?: info["gateway_nrp"] == true
          }
          |> add_urls(info["gateway_urls"])
          |> maybe_tls()

        write(state, ["CONNECT ", Jason.encode!(gateway_connect(state)), "\r\n", info_frame()])
        established(state)
    end
  end

  # Outbound: async INFO: commands, gossip, URL updates.
  defp handle_op({:info, info}, %{role: :outbound, phase: :connected} = state) do
    cmd = info["gateway_cmd"] || 0

    cond do
      cmd == @cmd_all_subs_start ->
        with_cmd_account(state, info, &all_subs_start/2)

      cmd == @cmd_all_subs_complete ->
        with_cmd_account(state, info, &all_subs_complete/2)

      cmd == @cmd_gossip and info["gateway"] not in [nil, state.name] ->
        implicit_connect(info["gateway"], info["gateway_urls"])
        {:ok, state}

      info["gateway"] == state.name ->
        {:ok, add_urls(state, info["gateway_urls"])}

      true ->
        {:ok, state}
    end
  end

  # Inbound: CONNECT then the remote's INFO.
  defp handle_op({:connect, opts}, %{role: :inbound, phase: :await_connect} = state) do
    gw = opts["gateway"]
    configured = Enum.any?(configured_gateways(), &(&1.name == gw))

    cond do
      gw in [nil, ""] ->
        write(state, "-ERR 'Attempted To Connect To Gateway Port'\r\n")
        {:close, :not_a_gateway, state}

      gw == name() ->
        write(state, "-ERR 'Gateway name #{inspect(gw)} is our own'\r\n")
        {:close, :own_name, state}

      Config.get(:gateway_reject_unknown) == true and not configured ->
        write(state, "-ERR 'Connection to gateway \"#{name()}\" rejected'\r\n")
        {:close, :unknown_gateway, state}

      not authorized?(opts) ->
        write(state, "-ERR 'Authorization Violation'\r\n")
        {:close, :unauthorized, state}

      true ->
        {:ok, %{state | name: gw, remote_id: opts["name"], phase: :await_peer_info}}
    end
  end

  defp handle_op({:info, info}, %{role: :inbound, phase: :await_peer_info} = state) do
    state = cancel(state, :auth_timeout)

    state = %{
      state
      | remote: info,
        name: info["gateway"] || state.name,
        remote_id: info["server_id"] || state.remote_id,
        remote_name: info["server_name"] || info["server_id"],
        headers?: info["headers"] == true,
        iom?: Config.get(:gateway_interest_only, true) != false
    }

    Registry.register(Natsbeam.Registry, {:gateway_in, state.name, self()}, nil)
    {:ok, state} = established(state)

    # nats-server: register, then dial back if we have no outbound to it
    # (implicit gateway), then gossip the other gateways we know about.
    implicit_connect(state.name, info["gateway_urls"])
    gossip(state)
    {:ok, state}
  end

  defp handle_op({:info, _}, state), do: {:ok, state}
  defp handle_op({:connect, _}, state), do: {:ok, state}

  defp handle_op(:ping, state) do
    write(state, "PONG\r\n")
    {:ok, state}
  end

  defp handle_op(:pong, state), do: {:ok, %{state | pings_out: 0}}

  defp handle_op({:err, msg}, state) do
    Logger.warning("natsbeam: gateway #{inspect(state.name)} sent -ERR '#{msg}'")

    if String.contains?(msg, "rejected") or String.contains?(msg, "wrong port"),
      do: {:close, {:fatal, msg}, state},
      else: {:close, {:err, msg}, state}
  end

  # -- interest, received on the outbound connection
  defp handle_op({:rs_plus, acc, subject, queue, _w}, %{role: :outbound} = state),
    do: {:ok, rs_plus(state, acc, subject, queue)}

  defp handle_op({:rs_minus, acc, subject, queue}, %{role: :outbound} = state),
    do: {:ok, rs_minus(state, acc, subject, queue)}

  defp handle_op({:a_plus, acc}, %{role: :outbound} = state), do: {:ok, a_plus(state, acc)}
  defp handle_op({:a_minus, acc}, %{role: :outbound} = state), do: {:ok, a_minus(state, acc)}

  # -- messages, received on the inbound connection
  defp handle_op(
         {:rmsg, acc, subject, reply, queues, headers, payload},
         %{role: :inbound, phase: :connected} = state
       ),
       do:
         {:ok, inbound_msg(bump(state, :in_msgs), acc, subject, reply, queues, headers, payload)}

  defp handle_op(op, state) do
    if elem(op, 0) in [:rs_plus, :rs_minus, :a_plus, :a_minus, :rmsg],
      do: Logger.debug("natsbeam: gateway #{state.role} ignoring #{elem(op, 0)}")

    {:ok, state}
  end

  # ----------------------------------------------------------- established

  defp established(state) do
    state =
      %{state | phase: :connected, pings_out: 0}
      |> schedule(:ping, @ping_interval)
      |> schedule(:stable, 2_000)

    :telemetry.attach(
      {__MODULE__, self()},
      [:natsbeam, :account, :start],
      &__MODULE__.on_account_start/4,
      self()
    )

    state =
      if state.role == :inbound do
        # nats-server sends all its queue subscriptions first, then switches
        # every account to interest-only (when advertising gateway_iom).
        interests =
          for acc <- Account.list() do
            {:ok, interest} = Account.watch_local_interest(acc, self())
            {acc, interest}
          end

        state = %{state | watched: MapSet.new(for {acc, _} <- interests, do: acc)}
        state = Enum.reduce(interests, state, fn {acc, i}, st -> send_queue_subs(st, acc, i) end)

        if state.iom?,
          do:
            Enum.reduce(interests, state, fn {acc, i}, st ->
              switch_to_interest_only(st, acc, i)
            end),
          else: state
      else
        Enum.reduce(Account.list(), state, &watch_account(&2, &1, false))
      end

    Logger.info(
      "natsbeam: gateway #{state.role} connection with #{inspect(state.name)} (#{state.remote_name}) registered"
    )

    :telemetry.execute([:natsbeam, :link, :up], %{}, %{kind: :gateway, remote: state.name})
    {:ok, state}
  end

  @doc false
  def on_account_start(_event, _m, %{account: name}, pid), do: send(pid, {:account_started, name})

  defp watch_account(state, acc, new?) do
    already = MapSet.member?(state.watched, acc)
    {:ok, interest} = Account.watch_local_interest(acc, self())
    state = %{state | watched: MapSet.put(state.watched, acc)}

    case state.role do
      :outbound ->
        # A (re)started account has an empty sublist: register our subscriptions again.
        state = if already or new?, do: %{state | reg: Map.delete(state.reg, acc)}, else: state
        sync_registrations(state, acc)

      :inbound ->
        # A new account (nats-server: a newly fetched account is switched too).
        state = if already, do: state, else: send_queue_subs(state, acc, interest)

        if state.iom? and not interest_only?(state, acc),
          do: switch_to_interest_only(state, acc, interest),
          else: state
    end
  end

  # ======================================================= outbound: interest

  defp new_entry, do: %{mode: :optimistic, ni: MapSet.new(), sl: MapSet.new(), qsubs: 0}

  # processGatewayRSub
  defp rs_plus(state, acc, subject, queue) do
    {e, use_sl} =
      case state.out[acc] do
        %{} = e -> {e, queue != nil or e.mode != :optimistic}
        _ when queue == nil -> {nil, nil}
        _ -> {new_entry(), true}
      end

    cond do
      e == nil ->
        state

      use_sl ->
        if MapSet.member?(e.sl, {subject, queue}) do
          state
        else
          e = %{
            e
            | sl: MapSet.put(e.sl, {subject, queue}),
              qsubs: e.qsubs + if(queue, do: 1, else: 0)
          }

          state
          |> put_out(acc, e)
          |> register(acc, {:sub, subject, queue}, subject, queue)
          |> sync_catch_all(acc)
        end

      true ->
        ni =
          if Subject.literal?(subject),
            do: MapSet.delete(e.ni, subject),
            else: MapSet.reject(e.ni, &Subject.matches?(subject, &1))

        state |> put_out(acc, %{e | ni: ni}) |> sync_catch_all(acc)
    end
  end

  # processGatewayRUnsub
  defp rs_minus(state, acc, subject, queue) do
    case state.out[acc] do
      %{} = e when queue != nil or e.mode != :optimistic ->
        if MapSet.member?(e.sl, {subject, queue}) do
          e = %{
            e
            | sl: MapSet.delete(e.sl, {subject, queue}),
              qsubs: e.qsubs - if(queue, do: 1, else: 0)
          }

          state = unregister(state, acc, {:sub, subject, queue})

          if MapSet.size(e.sl) == 0 and e.mode == :optimistic and e.ni != nil and
               MapSet.size(e.ni) == 0,
             do: state |> drop_out(acc) |> sync_catch_all(acc),
             else: state |> put_out(acc, e) |> sync_catch_all(acc)
        else
          state
        end

      %{} = e ->
        state |> put_out(acc, %{e | ni: MapSet.put(e.ni, subject)}) |> sync_catch_all(acc)

      _ when queue != nil ->
        state

      _ ->
        e = new_entry()
        state |> put_out(acc, %{e | ni: MapSet.put(e.ni, subject)}) |> sync_catch_all(acc)
    end
  end

  # processGatewayAccountUnsub: with queue subscriptions the entry stays and
  # its no-interest set is reset; otherwise the whole account is "no interest".
  defp a_minus(state, acc) do
    case state.out[acc] do
      %{qsubs: q} = e when q > 0 ->
        put_out(state, acc, %{e | ni: MapSet.new()}) |> sync_catch_all(acc)

      :none ->
        state

      e ->
        state = drop_plain_subs(state, acc, e)
        %{state | out: Map.put(state.out, acc, :none)} |> sync_catch_all(acc)
    end
  end

  # processGatewayAccountSub
  defp a_plus(state, acc) do
    case state.out[acc] do
      %{qsubs: q} when q > 0 -> state
      nil -> state
      e -> state |> drop_plain_subs(acc, e) |> drop_out(acc) |> sync_catch_all(acc)
    end
  end

  defp drop_plain_subs(state, acc, %{sl: sl}) do
    Enum.reduce(sl, state, fn {s, q}, st -> unregister(st, acc, {:sub, s, q}) end)
  end

  defp drop_plain_subs(state, _acc, _), do: state

  defp with_cmd_account(state, info, fun) do
    case info["gateway_cmd_payload"] && Base.decode64(info["gateway_cmd_payload"]) do
      {:ok, acc} when acc != "" ->
        {:ok, fun.(state, acc)}

      _ ->
        write(state, "-ERR 'Account absent from receive-all-subscriptions command'\r\n")
        {:close, :protocol_violation, state}
    end
  end

  # gatewayAllSubsReceiveStart
  defp all_subs_start(state, acc) do
    e =
      case state.out[acc] do
        %{} = e -> %{e | mode: :transitioning}
        _ -> %{new_entry() | mode: :transitioning, ni: nil}
      end

    state |> put_out(acc, e) |> sync_catch_all(acc)
  end

  # gatewayAllSubsReceiveComplete
  defp all_subs_complete(state, acc) do
    case state.out[acc] do
      %{} = e ->
        state |> put_out(acc, %{e | mode: :interest_only, ni: nil}) |> sync_catch_all(acc)

      _ ->
        state
    end
  end

  defp put_out(state, acc, e), do: %{state | out: Map.put(state.out, acc, e)}
  defp drop_out(state, acc), do: %{state | out: Map.delete(state.out, acc)}

  # Without explicit interest information an account is sent everything
  # (optimistic) unless the peer announced interest-only mode: that is a
  # catch-all subscription on ">" filtered by the no-interest set.
  defp catch_all?(state, acc) do
    not state.iom? and
      case state.out[acc] do
        nil -> true
        :none -> false
        %{ni: ni} -> ni != nil
      end
  end

  defp sync_catch_all(state, acc) do
    want = catch_all?(state, acc) and acc in state.watched
    have = MapSet.member?(Map.get(state.reg, acc, MapSet.new()), :all)

    cond do
      want and not have -> register(state, acc, :all, ">", nil)
      have and not want -> unregister(state, acc, :all)
      true -> state
    end
  end

  # Bring the account's sublist in line with our remote-interest view.
  defp sync_registrations(state, acc) do
    state =
      case state.out[acc] do
        %{sl: sl} ->
          Enum.reduce(sl, state, fn {s, q}, st -> register(st, acc, {:sub, s, q}, s, q) end)

        _ ->
          state
      end

    sync_catch_all(state, acc)
  end

  defp register(state, acc, key, subject, queue) do
    case Account.lookup(acc) do
      {:ok, apid, _} ->
        Account.subscribe(apid, self(), {:link, {:gw, acc, key}}, subject, queue)
        %{state | reg: Map.update(state.reg, acc, MapSet.new([key]), &MapSet.put(&1, key))}

      :error ->
        state
    end
  end

  defp unregister(state, acc, key) do
    if MapSet.member?(Map.get(state.reg, acc, MapSet.new()), key) do
      case Account.lookup(acc) do
        {:ok, apid, _} -> Account.unsubscribe(apid, self(), {:link, {:gw, acc, key}})
        :error -> :ok
      end

      %{state | reg: Map.update!(state.reg, acc, &MapSet.delete(&1, key))}
    else
      state
    end
  end

  # gatewayInterest: plain interest for this message?
  defp plain_interest?(state, acc, subject, keys) do
    explicit = Enum.any?(keys, &match?({:gw, _, {:sub, _, nil}}, &1))

    case state.out[acc] do
      :none -> false
      nil -> not state.iom?
      %{ni: ni} -> explicit or (not state.iom? and ni != nil and not MapSet.member?(ni, subject))
    end
  end

  # ======================================================= outbound: messages

  defp send_message(state, [{:gw, acc, _} | _] = keys, subject, reply, headers, payload) do
    routed =
      Enum.find_value(keys, fn
        {:gw, _, {:reply, r}} -> r
        _ -> nil
      end)

    {frame_subject, queues, send?} =
      if routed do
        # A reply to a request that came from this gateway with a `_GR_.` reply:
        # sent to the originating cluster only, regardless of interest.
        {routed, [], true}
      else
        qs =
          keys
          |> Enum.flat_map(fn
            {:gw, _, {:sub, _, q}} when q != nil -> [q]
            _ -> []
          end)
          |> Enum.uniq()

        psi = plain_interest?(state, acc, subject, keys)
        {subject, qs, psi or qs != []}
      end

    if send? do
      {reply, state} = map_reply(state, acc, reply)
      headers = if state.headers?, do: headers
      write(state, msg_frame(acc, frame_subject, reply, queues, headers, payload))
      bump(state, :out_msgs)
    else
      state
    end
  end

  defp send_message(state, _, _, _, _, _), do: state

  # Remember subscriptions made in the last couple of seconds: a request whose
  # reply matches one gets the `_GR_.` routed reply (shouldMapReplyForGatewaySend).
  defp note_recent(state, acc, subject, nil, weight) when weight > 0 do
    now = System.monotonic_time(:millisecond)

    recent =
      [{subject, now} | Map.get(state.recent, acc, [])]
      |> Enum.filter(fn {_, t} -> now - t <= @recent_sub_ms end)

    %{state | recent: Map.put(state.recent, acc, recent)}
  end

  defp note_recent(state, _, _, _, _), do: state

  defp map_reply(state, _acc, nil), do: {nil, state}

  defp map_reply(%{nrp?: true} = state, acc, reply) do
    # The subscription notice comes from the account process and the publish
    # from the client: different senders, so take in any notice already sent.
    state = drain_interest(state)
    now = System.monotonic_time(:millisecond)

    recent? =
      Enum.any?(Map.get(state.recent, acc, []), fn {s, t} ->
        now - t <= @recent_sub_ms and Subject.matches?(s, reply)
      end)

    if recent? and not String.starts_with?(reply, "_GR_."),
      do: {reply_prefix() <> reply, state},
      else: {reply, state}
  end

  defp map_reply(state, _acc, reply), do: {reply, state}

  defp drain_interest(state) do
    receive do
      {:local_interest, acc, subject, queue, weight} ->
        drain_interest(note_recent(state, acc, subject, queue, weight))
    after
      0 -> state
    end
  end

  # ======================================================== inbound: interest

  defp interest_only?(state, acc), do: match?(%{mode: :interest_only}, state.insim[acc])

  # Interest we never advertise: our own JetStream API (both clusters run one).
  defp advertise?(subject),
    do: subject != "$JS.API.>" and not String.starts_with?(subject, "_GR_.")

  defp send_queue_subs(state, acc, interest) do
    Enum.reduce(interest, state, fn
      {s, q, w}, st when q != nil -> inbound_interest(st, acc, s, q, w)
      _, st -> st
    end)
  end

  defp inbound_interest(state, acc, subject, queue, weight) do
    key = {acc, subject, queue}
    prev = Map.get(state.sent, key, 0)

    cond do
      not advertise?(subject) ->
        state

      queue != nil ->
        # Queue subscriptions are always sent (sendQueueSubOrUnsubToGateways);
        # a first queue sub silently clears a previous A-.
        cond do
          prev == 0 and weight > 0 ->
            write(state, ["RS+ ", acc, " ", subject, " ", queue, " 1\r\n"])

            state = if state.insim[acc] == :a_minus, do: drop_insim(state, acc), else: state
            %{state | sent: Map.put(state.sent, key, weight)} |> bump(:rs_plus)

          prev > 0 and weight == 0 ->
            write(state, ["RS- ", acc, " ", subject, " ", queue, "\r\n"])
            %{state | sent: Map.delete(state.sent, key)} |> bump(:rs_minus)

          weight > 0 ->
            %{state | sent: Map.put(state.sent, key, weight)}

          true ->
            state
        end

      true ->
        plain_interest_changed(state, acc, subject, key, prev, weight)
    end
  end

  # maybeSendSubOrUnsubToGateways
  defp plain_interest_changed(state, acc, subject, key, prev, weight) do
    case state.insim[acc] do
      %{mode: :interest_only} ->
        cond do
          prev == 0 and weight > 0 ->
            write(state, ["RS+ ", acc, " ", subject, "\r\n"])
            %{state | sent: Map.put(state.sent, key, weight)} |> bump(:rs_plus)

          prev > 0 and weight == 0 ->
            write(state, ["RS- ", acc, " ", subject, "\r\n"])
            %{state | sent: Map.delete(state.sent, key)} |> bump(:rs_minus)

          weight > 0 ->
            %{state | sent: Map.put(state.sent, key, weight)}

          true ->
            state
        end

      %{mode: :optimistic, ni: ni} = e when weight > 0 ->
        # A new subscription clears a previous RS- (a wildcard clears every
        # subject it covers, and is sent as is).
        {cleared, ni} =
          if Subject.literal?(subject) do
            {MapSet.member?(ni, subject), MapSet.delete(ni, subject)}
          else
            {gone, kept} = Enum.split_with(ni, &Subject.matches?(subject, &1))
            {gone != [], MapSet.new(kept)}
          end

        if cleared do
          write(state, ["RS+ ", acc, " ", subject, "\r\n"])
          %{state | insim: Map.put(state.insim, acc, %{e | ni: ni})} |> bump(:rs_plus)
        else
          state
        end

      :a_minus when weight > 0 ->
        write(state, ["A+ ", acc, "\r\n"])
        state |> drop_insim(acc) |> bump(:a_plus)

      _ ->
        state
    end
  end

  defp drop_insim(state, acc), do: %{state | insim: Map.delete(state.insim, acc)}

  # gatewaySwitchAccountToSendAllSubs: INFO cmd 2, every plain subscription, INFO cmd 3.
  defp switch_to_interest_only(state, acc, interest \\ nil) do
    interest =
      interest ||
        case Account.lookup(acc) do
          {:ok, _, sublist} -> Sublist.local_interest(sublist)
          :error -> []
        end

    write(state, cmd_frame(@cmd_all_subs_start, acc))

    state =
      Enum.reduce(interest, state, fn
        {s, nil, w}, st ->
          if advertise?(s) do
            write(st, ["RS+ ", acc, " ", s, "\r\n"])
            %{st | sent: Map.put(st.sent, {acc, s, nil}, w)} |> bump(:rs_plus)
          else
            st
          end

        _, st ->
          st
      end)

    write(state, cmd_frame(@cmd_all_subs_complete, acc))
    Logger.debug("natsbeam: gateway #{inspect(state.name)}: account #{acc} now interest-only")
    %{state | insim: Map.put(state.insim, acc, %{mode: :interest_only})}
  end

  # ======================================================== inbound: messages

  defp inbound_msg(state, acc, "_GR_." <> _ = subject, reply, _queues, headers, payload)
       when byte_size(subject) > 19 do
    <<"_GR_.", ch::binary-size(6), ".", _sh::binary-size(6), ".", real::binary>> = subject

    # A reply routed back to us (handleGatewayReply): deliver under the real
    # subject to every subscriber; one for another cluster is dropped.
    if ch == hash(name()) do
      case Account.lookup(acc) do
        {:ok, _, sublist} ->
          Account.publish_from_gateway(sublist, real, real, reply, headers, payload, nil)
          state

        :error ->
          send_a_minus(state, acc)
      end
    else
      state
    end
  end

  defp inbound_msg(state, acc, subject, reply, queues, headers, payload) do
    case Account.lookup(acc) do
      :error ->
        send_a_minus(state, acc)

      {:ok, _, sublist} ->
        {reply, state} = map_inbound_reply(state, acc, reply)

        # JetStream deliveries: `$JS.ACK...@<subject>` (same convention as routes).
        {match_subject, subject, reply} =
          case reply && js_ack_split(reply) do
            {ack, original} -> {subject, original, ack}
            _ -> {subject, subject, reply}
          end

        js_api? = String.starts_with?(match_subject, "$JS.API.")

        plain? =
          not js_api? and
            (interest_only?(state, acc) or local_plain_interest?(sublist, match_subject))

        state = if plain?, do: state, else: no_interest(state, acc, sublist, match_subject)

        cond do
          js_api? ->
            state

          not plain? and queues == [] ->
            state

          true ->
            Account.publish_from_gateway(
              sublist,
              match_subject,
              subject,
              reply,
              headers,
              payload,
              queues
            )

            state
        end
    end
  end

  defp local_plain_interest?(sublist, subject) do
    Enum.any?(Sublist.match(sublist, subject), fn {_, sid, q} ->
      is_nil(q) and not Sublist.link_sid?(sid)
    end)
  end

  # A `_GR_.<cluster>.<server>.<reply>` reply: local subscribers see the plain
  # reply; a publish on it within a few seconds goes back to the origin cluster
  # with the routed subject, through our outbound connection to that cluster.
  defp map_inbound_reply(state, _acc, nil), do: {nil, state}

  defp map_inbound_reply(state, acc, "_GR_." <> _ = routed) when byte_size(routed) > 19 do
    <<"_GR_.", ch::binary-size(6), ".", _sh::binary-size(6), ".", real::binary>> = routed

    with opid when is_pid(opid) <- outbound_by_hash(ch),
         {:ok, apid, _} <- Account.lookup(acc) do
      key = {acc, routed}

      state =
        case state.replies[key] do
          {_, _, t} ->
            Process.cancel_timer(t)
            state

          nil ->
            Account.subscribe(apid, opid, {:link, {:gw, acc, {:reply, routed}}}, real, nil)
            state
        end

      t = Process.send_after(self(), {:expire_reply, key}, @reply_map_ttl)
      {real, %{state | replies: Map.put(state.replies, key, {apid, opid, t})}}
    else
      _ -> {real, state}
    end
  end

  defp map_inbound_reply(state, _acc, reply), do: {reply, state}

  # gatewayHandleSubjectNoInterest / gatewayHandleAccountNoInterest
  defp no_interest(state, acc, sublist, subject) do
    e = state.insim[acc]

    cond do
      # Other BEAM nodes may have subscribers we cannot see: stay optimistic.
      beam_peers?(acc) ->
        state

      e == :a_minus or match?(%{mode: :interest_only}, e) ->
        state

      e != nil and MapSet.member?(e.ni, subject) ->
        state

      not Sublist.local_subscriptions?(sublist) ->
        send_a_minus(state, acc)

      true ->
        e = e || %{mode: :optimistic, ni: MapSet.new()}

        if MapSet.size(e.ni) >= Config.get(:gateway_max_runsub, 1_000) do
          switch_to_interest_only(state, acc)
        else
          write(state, ["RS- ", acc, " ", subject, "\r\n"])

          %{state | insim: Map.put(state.insim, acc, %{e | ni: MapSet.put(e.ni, subject)})}
          |> bump(:rs_minus)
        end
    end
  end

  defp send_a_minus(state, acc) do
    if state.insim[acc] == :a_minus or beam_peers?(acc) do
      state
    else
      write(state, ["A- ", acc, "\r\n"])
      %{state | insim: Map.put(state.insim, acc, :a_minus)} |> bump(:a_minus)
    end
  end

  defp beam_peers?(acc) do
    Enum.any?(:pg.get_members(Natsbeam.PG, {:account, acc}), &(node(&1) != node()))
  catch
    :exit, _ -> false
  end

  # Split a `$JS.ACK.<s>.<c>.<d>.<ss>.<cs>.<t>.<p>@<deliver>` reply at the first
  # '@' after the eight dots of the ACK token.
  defp js_ack_split("$JS.ACK." <> _ = reply), do: find_at(reply, 0, 0)
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

  # ------------------------------------------------------- implicit / gossip

  defp implicit_connect(gw, urls) when is_binary(gw) and is_list(urls) and urls != [] do
    if gw != name() and outbound_pid(gw) == nil do
      [first | _] = urls

      case connect("nats://" <> first, name: gw, implicit: true, urls: urls) do
        {:ok, _} -> Logger.info("natsbeam: implicit gateway connection to #{inspect(gw)}")
        _ -> :ok
      end
    end

    :ok
  end

  defp implicit_connect(_, _), do: :ok

  # gossipGatewaysToInboundGateway
  defp gossip(state) do
    outbound =
      Registry.select(Natsbeam.Registry, [
        {{{:gateway_out, :"$1"}, :_, :"$2"}, [], [{{:"$1", :"$2"}}]}
      ])

    for {gw, url} <- outbound, gw != state.name, is_binary(url) do
      info = %{
        "server_id" => Server.info().id,
        "gateway" => gw,
        "gateway_urls" => [url],
        "gateway_cmd" => @cmd_gossip
      }

      write(state, ["INFO ", Jason.encode!(info), "\r\n"])
    end

    :ok
  end

  # ------------------------------------------------------------- frames

  defp info_frame do
    srv = Server.info()
    cfg = Config.all()
    url = advertise()
    [host, port] = String.split(url, ":", parts: 2)
    auth = Config.get(:gateway_auth)

    info = %{
      "server_id" => srv.id,
      "server_name" => srv.name,
      "version" => cfg.advertised_version,
      "go" => "beam",
      "host" => host,
      "port" => String.to_integer(port),
      "auth_required" => auth != nil,
      "tls_required" => false,
      "max_payload" => cfg.max_payload,
      "headers" => true,
      "proto" => 1,
      "gateway" => name(),
      "gateway_nrp" => true,
      "gateway_iom" => Config.get(:gateway_interest_only, true) != false
    }

    # Without a gateway listener there is nothing to dial back: a remote that
    # is not configured with our URL then has no outbound connection to us.
    info =
      if port == "0",
        do: info,
        else: Map.merge(info, %{"gateway_url" => url, "gateway_urls" => [url]})

    ["INFO ", Jason.encode!(info), "\r\n"]
  end

  defp cmd_frame(cmd, acc) do
    info = %{
      "gateway" => name(),
      "gateway_cmd" => cmd,
      "gateway_cmd_payload" => Base.encode64(acc)
    }

    ["INFO ", Jason.encode!(info), "\r\n"]
  end

  @doc false
  # The URL other clusters use to reach our gateway port.
  def advertise do
    case Config.get(:gateway_advertise) do
      adv when is_binary(adv) and adv != "" ->
        if String.contains?(adv, ":"), do: adv, else: "#{adv}:#{gateway_port()}"

      _ ->
        "#{advertise_host()}:#{gateway_port()}"
    end
  end

  defp gateway_port do
    case Natsbeam.Link.Acceptor.port(:gateway) do
      {:ok, p} -> p
      _ -> Config.get(:gateway_port) || 0
    end
  end

  defp advertise_host do
    case Config.get(:gateway_host) do
      h when h in [nil, "0.0.0.0", "::"] -> first_non_loopback() || "127.0.0.1"
      h -> h
    end
  end

  defp first_non_loopback do
    case :inet.getifaddrs() do
      {:ok, ifs} ->
        Enum.find_value(ifs, fn {_name, opts} ->
          flags = Keyword.get(opts, :flags, [])

          if :loopback not in flags and :up in flags do
            Enum.find_value(Keyword.get_values(opts, :addr), fn
              {a, b, c, d} -> "#{a}.#{b}.#{c}.#{d}"
              _ -> nil
            end)
          end
        end)

      _ ->
        nil
    end
  end

  defp gateway_connect(state) do
    %{
      "verbose" => false,
      "pedantic" => false,
      "tls_required" => state.transport == :ssl,
      "name" => Server.info().id,
      "gateway" => name(),
      "headers" => true
    }
    |> then(fn c ->
      case {state.opts[:user], state.opts[:pass]} do
        {nil, _} -> c
        {u, p} -> Map.merge(c, %{"user" => u, "pass" => p || ""})
      end
    end)
  end

  defp msg_frame(acc, subject, reply, queues, headers, payload) do
    op = if headers, do: "HMSG ", else: "RMSG "

    targets =
      case {reply, queues} do
        {nil, []} -> []
        {r, []} -> [r, " "]
        {nil, qs} -> ["| ", Enum.intersperse(qs, " "), " "]
        {r, qs} -> ["+ ", r, " ", Enum.intersperse(qs, " "), " "]
      end

    sizes =
      if headers do
        hl = byte_size(headers)
        [Integer.to_string(hl), " ", Integer.to_string(hl + byte_size(payload))]
      else
        Integer.to_string(byte_size(payload))
      end

    [op, acc, " ", subject, " ", targets, sizes, "\r\n", headers || "", payload, "\r\n"]
  end

  # ------------------------------------------------------------- helpers

  defp configured_gateways do
    for g <- Config.get(:gateways) || [], g = Map.new(g), do: %{g | name: to_string(g[:name])}
  end

  defp authorized?(opts) do
    case Config.get(:gateway_auth) do
      nil ->
        true

      auth ->
        auth = Map.new(auth)

        opts["user"] == auth[:user] and
          Natsbeam.Auth.secret_match?(auth[:pass] || "", opts["pass"] || "")
    end
  end

  defp add_urls(state, urls) when is_list(urls) do
    extra =
      for u <- urls, is_binary(u), hp = parse_host_port(u), hp != nil, do: hp

    %{state | urls: Enum.uniq(state.urls ++ extra)}
  end

  defp add_urls(state, _), do: state

  defp parse_host_port(s) do
    case String.split(s, ":") do
      [h, p] ->
        case Integer.parse(p) do
          {n, ""} -> {h, n}
          _ -> nil
        end

      _ ->
        nil
    end
  end

  defp host_port(%URI{host: h, port: p}), do: {h || "127.0.0.1", p || 7222}
  defp host_port_string(uri), do: (fn {h, p} -> "#{h}:#{p}" end).(host_port(uri))

  defp userinfo(%URI{userinfo: nil}), do: []

  defp userinfo(%URI{userinfo: ui}) do
    case String.split(ui, ":", parts: 2) do
      [user, pass] -> [user: user, pass: pass]
      [user] -> [user: user]
    end
  end

  defp maybe_tls(%{remote: %{"tls_required" => true}, transport: :gen_tcp} = state) do
    opts = Keyword.get(state.opts, :tls, []) |> Keyword.put_new(:verify, :verify_none)

    case :ssl.connect(state.socket, opts, 5_000) do
      {:ok, ssl} ->
        :ssl.setopts(ssl, active: :once)
        %{state | socket: ssl, transport: :ssl}

      {:error, reason} ->
        Logger.warning("natsbeam: gateway TLS upgrade failed: #{inspect(reason)}")
        state
    end
  end

  defp maybe_tls(state), do: state

  defp write(%{socket: nil}, _), do: :ok

  defp write(state, iodata) do
    case state.transport.send(state.socket, iodata) do
      :ok -> :ok
      {:error, _} -> :ok
    end
  end

  defp setopts(%{transport: :ssl} = state, opts), do: :ssl.setopts(state.socket, opts)
  defp setopts(state, opts), do: :inet.setopts(state.socket, opts)

  defp detach(state) do
    if state.role == :outbound do
      for {acc, keys} <- state.reg, MapSet.size(keys) > 0 do
        case Account.lookup(acc) do
          {:ok, apid, _} -> Account.unsubscribe_all(apid, self())
          :error -> :ok
        end
      end
    end

    for {{acc, routed}, {apid, opid, t}} <- state.replies do
      Process.cancel_timer(t)

      try do
        Account.unsubscribe(apid, opid, {:link, {:gw, acc, {:reply, routed}}})
      catch
        :exit, _ -> :ok
      end
    end

    for acc <- state.watched, do: Account.unwatch_local_interest(acc, self())
    :telemetry.detach({__MODULE__, self()})

    %{
      state
      | out: %{},
        reg: %{},
        recent: %{},
        insim: %{},
        sent: %{},
        replies: %{},
        watched: MapSet.new()
    }
  end

  defp close(state, reason) do
    if state.socket, do: state.transport.close(state.socket)
    state = detach(%{state | socket: nil, phase: :connecting})
    state = Enum.reduce([:ping, :stable, :info_timeout, :auth_timeout], state, &cancel(&2, &1))

    :telemetry.execute([:natsbeam, :link, :down], %{}, %{
      kind: :gateway,
      remote: state.name,
      reason: reason
    })

    case {state.role, reason} do
      {:inbound, _} ->
        {:stop, :normal, state}

      {:outbound, {:fatal, msg}} ->
        Logger.error("natsbeam: gateway #{inspect(state.name)} at #{state.url} refused: #{msg}")
        {:stop, :normal, state}

      {:outbound, _} ->
        retry(state)
    end
  end

  # nats-server retries an explicit gateway forever (1s between attempts);
  # an implicit one is given up once the remote no longer connects to us.
  defp retry(%{implicit?: true} = state) do
    if state.attempt >= 3 and not inbound_from?(state.name) do
      Logger.info("natsbeam: giving up implicit gateway #{inspect(state.name)}")
      {:stop, :normal, state}
    else
      {:noreply, schedule(%{state | attempt: state.attempt + 1}, :redial, 1_000)}
    end
  end

  defp retry(state) do
    wait = 1_000 + :rand.uniform(100)
    {:noreply, schedule(%{state | attempt: state.attempt + 1}, :redial, wait)}
  end

  defp schedule(state, key, ms) do
    if t = state.timers[key], do: Process.cancel_timer(t)
    %{state | timers: Map.put(state.timers, key, Process.send_after(self(), key, ms))}
  end

  defp cancel(state, key) do
    if t = state.timers[key], do: Process.cancel_timer(t)
    %{state | timers: Map.delete(state.timers, key)}
  end

  defp bump(state, key), do: %{state | stats: Map.update(state.stats, key, 1, &(&1 + 1))}
end
