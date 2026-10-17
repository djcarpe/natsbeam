defmodule Natsbeam.Client do
  @moduledoc """
  One process per TCP/TLS client connection (a `ThousandIsland.Handler`).

  Speaks the NATS client protocol: sends `INFO`, authenticates the `CONNECT`,
  then handles `PUB`/`HPUB`/`SUB`/`UNSUB`/`PING`/`PONG`. Messages for this
  client's subscriptions arrive in the mailbox as
  `{:natsbeam_msg, sid, subject, reply, headers, payload}` and are written out
  as `MSG`/`HMSG` frames, batched when the mailbox is busy.
  """
  use ThousandIsland.Handler
  require Logger

  alias Natsbeam.{Account, Auth, Config, Permissions, Server, Subject}
  alias Natsbeam.Protocol.{Encoder, Parser}
  alias ThousandIsland.Socket

  defmodule State do
    @moduledoc false
    defstruct id: nil,
              listener: nil,
              phase: :await_connect,
              parser: nil,
              cfg: nil,
              nonce: nil,
              ip: nil,
              port: nil,
              tls?: false,
              tls_opts: nil,
              opts: %{},
              verbose?: false,
              pedantic?: false,
              headers?: false,
              no_responders?: false,
              echo?: true,
              identity: nil,
              account: nil,
              account_pid: nil,
              account_ref: nil,
              sublist: nil,
              subs: %{},
              resp_allow: %{},
              pings_out: 0,
              ping_timer: nil,
              auth_timer: nil,
              expiry_timer: nil,
              delivered: 0,
              in_msgs: 0,
              out_msgs: 0,
              in_bytes: 0,
              out_bytes: 0,
              # subject => Sublist.match/2 result, and the account's remote
              # peers, both valid for sublist generation match_gen
              match_cache: %{},
              match_gen: -1,
              peers: [],
              connected_at: nil
  end

  # Publishers cache the subscriber list per subject; the cache is dropped
  # whenever the account's interest changes (sublist generation) or when it
  # grows past this many subjects.
  @match_cache_max 1024

  # ------------------------------------------------------------- connection

  @impl ThousandIsland.Handler
  def handle_connection(socket, handler_opts) do
    cfg = Config.all()
    id = Server.next_client_id()
    {ip, port} = peer(socket)
    tls_opts = handler_opts[:tls]
    handshake_first? = handler_opts[:tls_handshake_first] == true

    state = %State{
      id: id,
      listener: handler_opts[:listener],
      cfg: cfg,
      ip: ip,
      port: port,
      tls?: handshake_first?,
      tls_opts: tls_opts,
      parser: Parser.new(max_control_line: cfg.max_control_line, max_payload: cfg.max_payload),
      nonce: if(Auth.nonce_required?(cfg.auth), do: Server.nonce()),
      connected_at: DateTime.utc_now()
    }

    register(state)
    Socket.send(socket, Encoder.info(info(state, socket)))
    auth_timer = Process.send_after(self(), :auth_timeout, cfg.auth_timeout)
    state = %{state | auth_timer: auth_timer}
    :telemetry.execute([:natsbeam, :client, :connect], %{}, %{id: id, ip: ip})

    if tls_opts && not handshake_first? do
      {:switch_transport, {ThousandIsland.Transports.SSL, tls_opts}, %{state | tls?: true}}
    else
      {:continue, state}
    end
  end

  defp peer(socket) do
    case Socket.peername(socket) do
      {:ok, {ip, port}} -> {:inet.ntoa(ip) |> to_string(), port}
      _ -> {nil, nil}
    end
  end

  defp info(%State{} = s, socket) do
    srv = Server.info()

    {host, port} =
      case Socket.sockname(socket) do
        {:ok, {h, p}} -> {to_string(:inet.ntoa(h)), p}
        _ -> {"0.0.0.0", 0}
      end

    base = %{
      server_id: srv.id,
      server_name: srv.name,
      version: s.cfg.advertised_version,
      natsbeam: srv.version,
      proto: 1,
      go: "",
      host: host,
      port: port,
      headers: true,
      max_payload: s.cfg.max_payload,
      client_id: s.id,
      client_ip: s.ip,
      jetstream: Natsbeam.JetStream.enabled?()
    }

    base
    |> maybe_put(:auth_required, Auth.required?(s.cfg.auth), true)
    |> maybe_put(:nonce, s.nonce, true)
    |> maybe_put(:tls_required, s.tls_opts != nil, true)
    |> maybe_put(:tls_available, s.tls_opts != nil, true)
  end

  defp maybe_put(map, _k, v, _) when v in [nil, false], do: map
  defp maybe_put(map, k, v, _), do: Map.put(map, k, v)

  # ------------------------------------------------------------------- data

  @impl ThousandIsland.Handler
  def handle_data(data, socket, %State{} = state) do
    state = bump(state, :in_bytes, byte_size(data))

    case Parser.parse(state.parser, data) do
      {:ok, ops, parser} ->
        case process(ops, [], %{state | parser: parser}) do
          {:ok, out, state} ->
            flush(socket, out, state)
            {:continue, state}

          {:close, out, state} ->
            flush(socket, out, state)
            {:close, state}
        end

      {:error, reason, parser} ->
        Socket.send(socket, Encoder.err(reason))
        {:close, %{state | parser: parser}}
    end
  end

  defp flush(_socket, [], _state), do: :ok

  defp flush(socket, out, _state) do
    Socket.send(socket, Enum.reverse(out))
  end

  # process/3 threads an output accumulator (reversed iodata) through the ops.
  defp process([], out, state), do: {:ok, out, state}

  defp process([op | rest], out, state) do
    case handle_op(op, state) do
      {:ok, state} -> process(rest, out, state)
      {:ok, frame, state} -> process(rest, [frame | out], state)
      {:close, frame, state} -> {:close, [frame | out], state}
    end
  end

  # -- CONNECT

  defp handle_op({:connect, opts}, %State{phase: :await_connect} = state) do
    client = %{
      id: state.id,
      ip: state.ip,
      tls?: state.tls?,
      connection_type: "STANDARD",
      listener: state.listener
    }

    case Auth.authenticate(opts, state.nonce, client) do
      {:ok, identity} -> finish_connect(opts, identity, state)
      {:error, reason} -> close_with(reason, state)
    end
  end

  defp handle_op({:connect, opts}, %State{phase: :connected} = state) do
    # Re-CONNECT after auth: nats-server accepts option updates (verbose etc.).
    state = apply_connect_opts(state, opts)
    ok(state)
  end

  # -- anything else before CONNECT: allowed only when no auth is required.

  defp handle_op(op, %State{phase: :await_connect} = state) do
    if Auth.required?(state.cfg.auth) do
      close_with("Authorization Violation", state)
    else
      {:ok, identity} = Auth.authenticate(%{}, nil, %{})

      case finish_connect(%{}, identity, state) do
        {:ok, state} -> handle_op(op, state)
        {:ok, _frame, state} -> handle_op(op, state)
        other -> other
      end
    end
  end

  # -- PING / PONG

  defp handle_op(:ping, state), do: {:ok, Encoder.pong(), state}
  defp handle_op(:pong, state), do: {:ok, %{state | pings_out: 0}}

  # -- PUB / HPUB

  defp handle_op({:pub, subject, reply, payload}, state),
    do: publish(subject, reply, nil, payload, state)

  defp handle_op({:hpub, subject, reply, headers, payload}, state),
    do: publish(subject, reply, headers, payload, state)

  # -- SUB

  defp handle_op({:sub, subject, queue, sid}, state) do
    cond do
      not Subject.valid_subscription?(subject) or (queue && not valid_queue?(queue)) ->
        {:ok, Encoder.err("Invalid Subject"), state}

      not Permissions.can_subscribe?(perms(state), subject, queue) ->
        :telemetry.execute([:natsbeam, :client, :permission_violation], %{}, %{
          id: state.id,
          op: :sub,
          subject: subject
        })

        msg = ~s(Permissions Violation for Subscription to "#{subject}")
        msg = if queue, do: msg <> ~s( using queue "#{queue}"), else: msg
        {:ok, Encoder.err(msg), state}

      max_subs_exceeded?(state) ->
        {:ok, Encoder.err("maximum subscriptions exceeded"), state}

      true ->
        case Account.subscribe(state.account_pid, self(), sid, subject, queue) do
          :ok ->
            sub = %{subject: subject, queue: queue, max: nil, received: 0}
            ok(%{state | subs: Map.put(state.subs, sid, sub)})

          {:error, :max_subscriptions} ->
            {:ok, Encoder.err("maximum subscriptions exceeded"), state}
        end
    end
  end

  # -- UNSUB

  defp handle_op({:unsub, sid, max}, state) do
    case Map.fetch(state.subs, sid) do
      :error ->
        ok(state)

      {:ok, sub} ->
        if is_nil(max) or sub.received >= max do
          ok(remove_sub(state, sid))
        else
          ok(%{state | subs: Map.put(state.subs, sid, %{sub | max: max})})
        end
    end
  end

  defp handle_op({:info, _}, state), do: {:ok, state}
  defp handle_op({:unknown, _}, state), do: close_with("Unknown Protocol Operation", state)

  # ----------------------------------------------------------------- connect

  defp finish_connect(opts, identity, state) do
    if state.auth_timer, do: Process.cancel_timer(state.auth_timer)

    with :ok <- check_tls(opts, state),
         :ok <- check_max_connections(state),
         {:ok, apid, sublist} <-
           Account.join(identity.account, self(), client_meta(state, identity)) do
      ref = Process.monitor(apid)
      cfg = state.cfg
      max_payload = Enum.min([cfg.max_payload, identity.max_payload || cfg.max_payload])
      parser = %{state.parser | max_payload: max_payload}

      state =
        %{
          state
          | phase: :connected,
            identity: identity,
            account: identity.account,
            account_pid: apid,
            account_ref: ref,
            sublist: sublist,
            parser: parser,
            auth_timer: nil
        }
        |> apply_connect_opts(opts)
        |> schedule_ping()
        |> schedule_expiry()

      register(state)

      :telemetry.execute([:natsbeam, :client, :authenticated], %{}, %{
        id: state.id,
        account: identity.account,
        user: identity.user,
        kind: identity.kind
      })

      ok(state)
    else
      {:error, :max_connections} ->
        close_with("maximum account active connections exceeded", state)

      {:error, :server_max_connections} ->
        close_with("Maximum Connections Exceeded", state)

      {:error, reason} when is_binary(reason) ->
        close_with(reason, state)
    end
  end

  defp check_tls(opts, state) do
    if state.tls_opts && not state.tls? && opts["tls_required"] != true do
      {:error, "Secure Connection - TLS Required"}
    else
      :ok
    end
  end

  defp check_max_connections(state) do
    max = state.cfg.max_connections

    if is_integer(max) and max > 0 and Natsbeam.Clients.count() > max,
      do: {:error, :server_max_connections},
      else: :ok
  end

  defp apply_connect_opts(state, opts) do
    protocol = Map.get(opts, "protocol", 0)

    %{
      state
      | opts: opts,
        verbose?: Map.get(opts, "verbose", false) == true,
        pedantic?: Map.get(opts, "pedantic", false) == true,
        headers?: Map.get(opts, "headers", false) == true,
        no_responders?: Map.get(opts, "no_responders", false) == true,
        echo?: Map.get(opts, "echo", true) != false and protocol >= 0
    }
  end

  defp schedule_ping(state) do
    if state.ping_timer, do: Process.cancel_timer(state.ping_timer)
    %{state | ping_timer: Process.send_after(self(), :ping, state.cfg.ping_interval)}
  end

  defp schedule_expiry(%State{identity: %{expires_at: exp}} = state)
       when is_integer(exp) and exp > 0 do
    ms = max((exp - System.os_time(:second)) * 1000, 0)
    %{state | expiry_timer: Process.send_after(self(), :auth_expired, ms)}
  end

  defp schedule_expiry(state), do: state

  # ----------------------------------------------------------------- publish

  defp publish(subject, reply, headers, payload, state) do
    cond do
      not Subject.valid_publish?(subject, state.pedantic?) ->
        {:ok,
         Encoder.err(if(state.pedantic?, do: "Invalid Publish Subject", else: "Invalid Subject")),
         state}

      reply != nil and not Subject.valid_publish?(reply, state.pedantic?) ->
        {:ok, Encoder.err("Invalid Subject"), state}

      not publish_allowed?(subject, state) ->
        :telemetry.execute([:natsbeam, :client, :permission_violation], %{}, %{
          id: state.id,
          op: :pub,
          subject: subject
        })

        {:ok, Encoder.err(~s(Permissions Violation for Publish to "#{subject}")), state}

      true ->
        state = consume_resp_allow(subject, state)
        state = bump(state, :in_msgs, 1)
        {matches, state} = cached_matches(subject, state)

        n =
          Account.publish_matches(
            matches,
            state.peers,
            subject,
            reply,
            headers,
            payload,
            self(),
            state.echo?
          )

        :telemetry.execute(
          [:natsbeam, :message, :publish],
          %{bytes: byte_size(payload), deliveries: n},
          %{
            account: state.account,
            subject: subject
          }
        )

        if n == 0 and reply != nil and state.no_responders? and state.headers? do
          no_responders(reply, state)
        end

        ok(state)
    end
  end

  # Subscriber list for `subject` (and the account's remote peers in
  # `state.peers`), from the per-connection cache when the account's interest
  # has not changed since it was cached: one atomic read per publish.
  defp cached_matches(subject, %State{sublist: sublist} = state) do
    gen = Natsbeam.Sublist.generation(sublist)

    if gen == state.match_gen do
      case state.match_cache do
        %{^subject => matches} -> {matches, state}
        cache -> cache_miss(subject, gen, cache, state)
      end
    else
      state = %{state | peers: Natsbeam.Sublist.remote_peers(sublist)}
      cache_miss(subject, gen, %{}, state)
    end
  end

  defp cache_miss(subject, gen, cache, state) do
    matches = Natsbeam.Sublist.match(state.sublist, subject)
    cache = if map_size(cache) >= @match_cache_max, do: %{}, else: cache
    # The subject is a slice of the socket buffer; copy it so the cache does
    # not pin whole receive buffers in memory.
    cache = Map.put(cache, :binary.copy(subject), matches)
    {matches, %{state | match_cache: cache, match_gen: gen}}
  end

  # A request with nobody listening: hand a 503 back to our own inbox subs.
  defp no_responders(reply, state) do
    matches =
      state.sublist
      |> Natsbeam.Sublist.match(reply)
      |> Enum.filter(fn {pid, _, _} -> pid == self() end)
      |> Enum.map(fn {pid, sid, _} -> {pid, sid, nil} end)

    Account.deliver(matches, reply, nil, Encoder.no_responders_headers(), "")
    :ok
  end

  defp publish_allowed?(subject, state) do
    Permissions.can_publish?(perms(state), subject) or Map.has_key?(state.resp_allow, subject)
  end

  defp consume_resp_allow(_subject, %State{resp_allow: ra} = state) when ra == %{}, do: state

  defp consume_resp_allow(subject, state) do
    case Map.fetch(state.resp_allow, subject) do
      {:ok, {remaining, expires}} ->
        now = System.monotonic_time(:millisecond)

        cond do
          expires != nil and now > expires ->
            %{state | resp_allow: Map.delete(state.resp_allow, subject)}

          remaining <= 1 ->
            %{state | resp_allow: Map.delete(state.resp_allow, subject)}

          true ->
            %{state | resp_allow: Map.put(state.resp_allow, subject, {remaining - 1, expires})}
        end

      :error ->
        state
    end
  end

  # When response permissions apply, remember reply subjects we are allowed to answer.
  defp track_reply(state, nil), do: state

  defp track_reply(
         %State{identity: %{permissions: %Permissions{response: %{max: max, ttl: ttl}}}} = state,
         reply
       ) do
    expires = if ttl > 0, do: System.monotonic_time(:millisecond) + ttl
    %{state | resp_allow: Map.put(state.resp_allow, reply, {max, expires})}
  end

  defp track_reply(state, _), do: state

  defp perms(%State{identity: %{permissions: p}}), do: p
  defp perms(_), do: nil

  defp max_subs_exceeded?(state) do
    limit =
      case state.identity do
        %{max_subscriptions: n} when is_integer(n) and n >= 0 -> n
        _ -> state.cfg.max_subscriptions
      end

    is_integer(limit) and limit > 0 and map_size(state.subs) >= limit
  end

  defp valid_queue?(queue), do: Subject.valid_subscription?(queue)

  defp remove_sub(state, sid) do
    if state.account_pid, do: Account.unsubscribe(state.account_pid, self(), sid)
    %{state | subs: Map.delete(state.subs, sid)}
  end

  # ---------------------------------------------------------------- replies

  defp ok(%State{verbose?: true} = state), do: {:ok, Encoder.ok(), state}
  defp ok(state), do: {:ok, state}

  defp close_with(reason, state) do
    :telemetry.execute([:natsbeam, :client, :error], %{}, %{id: state.id, reason: reason})
    {:close, Encoder.err(reason), %{state | phase: :closing}}
  end

  # --------------------------------------------------------------- mailbox

  @impl GenServer
  def handle_info({:natsbeam_msg, _, _, _, _, _} = msg, {socket, %State{} = state}) do
    {frames, state} = deliver(msg, [], state)
    {frames, state} = drain(frames, state, 256)

    case frames do
      [] ->
        {:noreply, {socket, state}}

      _ ->
        case Socket.send(socket, Enum.reverse(frames)) do
          :ok -> check_slow(socket, state)
          {:error, reason} -> {:stop, {:shutdown, reason}, {socket, state}}
        end
    end
  end

  def handle_info(:ping, {socket, %State{} = state}) do
    if state.pings_out >= state.cfg.ping_max_outstanding do
      Socket.send(socket, Encoder.err("Stale Connection"))
      {:stop, {:shutdown, :stale}, {socket, state}}
    else
      Socket.send(socket, Encoder.ping())
      {:noreply, {socket, schedule_ping(%{state | pings_out: state.pings_out + 1})}}
    end
  end

  def handle_info(:auth_timeout, {socket, %State{phase: :await_connect} = state}) do
    Socket.send(socket, Encoder.err("Authentication Timeout"))
    {:stop, {:shutdown, :auth_timeout}, {socket, state}}
  end

  def handle_info(:auth_timeout, {socket, state}), do: {:noreply, {socket, state}}

  def handle_info(:auth_expired, {socket, state}) do
    Socket.send(socket, Encoder.err("User Authentication Expired"))
    {:stop, {:shutdown, :auth_expired}, {socket, state}}
  end

  def handle_info({:natsbeam_close, reason}, {socket, state}) do
    Socket.send(socket, Encoder.err(reason))
    {:stop, {:shutdown, {:closed, reason}}, {socket, state}}
  end

  def handle_info({:natsbeam_send, iodata}, {socket, state}) do
    Socket.send(socket, iodata)
    {:noreply, {socket, state}}
  end

  def handle_info({:DOWN, ref, :process, _pid, _}, {socket, %State{account_ref: ref} = state}) do
    Socket.send(socket, Encoder.err("Account Removed"))
    {:stop, {:shutdown, :account_down}, {socket, state}}
  end

  def handle_info(_other, {socket, state}), do: {:noreply, {socket, state}}

  @impl GenServer
  def handle_call(:info, _from, {socket, state}),
    do: {:reply, client_meta(state, state.identity), {socket, state}}

  def handle_call(:subs, _from, {socket, state}), do: {:reply, state.subs, {socket, state}}

  defp drain(frames, state, 0), do: {frames, state}

  defp drain(frames, state, n) do
    receive do
      {:natsbeam_msg, _, _, _, _, _} = msg ->
        {frames, state} = deliver(msg, frames, state)
        drain(frames, state, n - 1)
    after
      0 -> {frames, state}
    end
  end

  defp deliver({:natsbeam_msg, sid, subject, reply, headers, payload}, frames, state) do
    case Map.fetch(state.subs, sid) do
      :error ->
        {frames, state}

      {:ok, sub} ->
        frame =
          cond do
            headers && state.headers? -> Encoder.hmsg(subject, sid, reply, headers, payload)
            true -> Encoder.msg(subject, sid, reply, payload)
          end

        received = sub.received + 1

        state =
          state
          |> bump(:out_msgs, 1)
          |> bump(:out_bytes, IO.iodata_length(frame))
          |> track_reply(reply)

        state =
          if sub.max && received >= sub.max do
            remove_sub(state, sid)
          else
            %{state | subs: Map.put(state.subs, sid, %{sub | received: received})}
          end

        {[frame | frames], %{state | delivered: state.delivered + 1}}
    end
  end

  defp check_slow(socket, state) do
    if rem(state.delivered, 64) == 0 do
      {:message_queue_len, len} = Process.info(self(), :message_queue_len)

      if len > state.cfg.max_pending_msgs do
        Socket.send(socket, Encoder.err("Slow Consumer"))
        :telemetry.execute([:natsbeam, :client, :slow_consumer], %{pending: len}, %{id: state.id})
        {:stop, {:shutdown, :slow_consumer}, {socket, state}}
      else
        {:noreply, {socket, state}}
      end
    else
      {:noreply, {socket, state}}
    end
  end

  # ---------------------------------------------------------------- closing

  @impl ThousandIsland.Handler
  def handle_close(_socket, state), do: cleanup(state)
  @impl ThousandIsland.Handler
  def handle_error(_reason, _socket, state), do: cleanup(state)
  @impl ThousandIsland.Handler
  def handle_shutdown(socket, state) do
    Socket.send(socket, Encoder.err("Server Shutdown"))
    cleanup(state)
  end

  @impl ThousandIsland.Handler
  def handle_timeout(_socket, state), do: cleanup(state)

  defp cleanup(%State{} = state) do
    # The account also monitors us, so this is best-effort tidying; the account
    # may legitimately be gone already (it was removed, which is why we're closing).
    if state.account_pid do
      try do
        Account.unsubscribe_all(state.account_pid, self())
        Account.leave(state.account, self())
      catch
        :exit, _ -> :ok
      end
    end

    :telemetry.execute([:natsbeam, :client, :disconnect], %{}, %{
      id: state.id,
      account: state.account
    })

    :ok
  end

  defp cleanup(_), do: :ok

  # ---------------------------------------------------------------- registry

  defp register(state) do
    meta = client_meta(state, state.identity)

    case Registry.register(Natsbeam.ClientRegistry, state.id, meta) do
      {:ok, _} ->
        :ok

      {:error, {:already_registered, _}} ->
        Registry.update_value(Natsbeam.ClientRegistry, state.id, fn _ -> meta end)
    end
  end

  defp client_meta(state, identity) do
    %{
      id: state.id,
      pid: self(),
      ip: state.ip,
      port: state.port,
      tls: state.tls?,
      listener: state.listener,
      name: state.opts["name"],
      lang: state.opts["lang"],
      version: state.opts["version"],
      account: identity && identity.account,
      user: identity && identity.user,
      nkey: identity && identity.nkey,
      kind: identity && identity.kind,
      phase: state.phase,
      subscriptions: map_size(state.subs),
      connected_at: state.connected_at,
      stats: %{
        in_msgs: state.in_msgs,
        out_msgs: state.out_msgs,
        in_bytes: state.in_bytes,
        out_bytes: state.out_bytes
      }
    }
  end

  defp bump(%State{in_msgs: c} = state, :in_msgs, n), do: %{state | in_msgs: c + n}
  defp bump(%State{out_msgs: c} = state, :out_msgs, n), do: %{state | out_msgs: c + n}
  defp bump(%State{in_bytes: c} = state, :in_bytes, n), do: %{state | in_bytes: c + n}
  defp bump(%State{out_bytes: c} = state, :out_bytes, n), do: %{state | out_bytes: c + n}
end
