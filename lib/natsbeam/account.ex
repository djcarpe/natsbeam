defmodule Natsbeam.Account do
  @moduledoc """
  One process per account. Owns the account's subscription index and tracks
  its connected clients. Accounts are created on demand (`ensure/1`) and can be
  removed at runtime, which disconnects their clients.

  Subscription writes go through this process; matching and delivery happen in
  the publisher's process straight against the ETS tables (see `publish/6`).
  """
  use GenServer, restart: :transient
  require Logger
  alias Natsbeam.Sublist

  defstruct name: nil,
            sublist: nil,
            clients: %{},
            monitors: %{},
            limits: %{},
            claims: nil,
            watchers: %{},
            local_watchers: %{},
            pg_ref: nil

  @type name :: binary()

  # ---------------------------------------------------------------- lifecycle

  def start_link(name), do: GenServer.start_link(__MODULE__, name, name: via(name))

  def via(name), do: {:via, Registry, {Natsbeam.Registry, {:account, name}}}

  @doc "Find or start the account process. Returns `{:ok, pid, sublist}`."
  @spec ensure(name()) :: {:ok, pid(), Sublist.t()}
  def ensure(name, attempts \\ 200)

  def ensure(name, attempts) when is_binary(name) do
    case lookup(name) do
      {:ok, _pid, nil} when attempts > 0 ->
        # Registered but still in init (another caller is starting it): wait for its tables.
        Process.sleep(5)
        ensure(name, attempts - 1)

      {:ok, pid, sublist} ->
        {:ok, pid, sublist}

      :error ->
        case DynamicSupervisor.start_child(Natsbeam.AccountSupervisor, {__MODULE__, name}) do
          {:ok, _pid} -> ensure(name)
          {:error, {:already_started, _}} -> ensure(name)
        end
    end
  end

  @doc "Look up a running account."
  @spec lookup(name()) :: {:ok, pid(), Sublist.t()} | :error
  def lookup(name) do
    case Registry.lookup(Natsbeam.Registry, {:account, name}) do
      [{pid, sublist}] -> {:ok, pid, sublist}
      [] -> :error
    end
  end

  @doc "Names of all running accounts."
  def list do
    Registry.select(Natsbeam.Registry, [{{{:account, :"$1"}, :_, :_}, [], [:"$1"]}])
    |> Enum.sort()
  end

  @doc "Stop an account, disconnecting its clients."
  def remove(name) do
    case lookup(name) do
      {:ok, pid, _} -> GenServer.call(pid, :shutdown)
      :error -> :ok
    end
  end

  # ---------------------------------------------------------------- client API

  @doc "Register a client connection with the account (enforces connection limits)."
  @spec join(name(), pid(), map()) :: {:ok, pid(), Sublist.t()} | {:error, term()}
  def join(name, pid, meta \\ %{}) do
    {:ok, apid, sublist} = ensure(name)

    case GenServer.call(apid, {:join, pid, meta}) do
      :ok -> {:ok, apid, sublist}
      {:error, _} = e -> e
    end
  end

  @doc false
  # Monitor a process that inserted into the sublist directly (used by
  # subscribers created during account init, which cannot call us).
  def adopt(name, pid) do
    case lookup(name) do
      {:ok, apid, _} -> GenServer.cast(apid, {:adopt, pid})
      :error -> :ok
    end
  end

  def leave(name, pid) do
    case lookup(name) do
      {:ok, apid, _} -> GenServer.cast(apid, {:leave, pid})
      :error -> :ok
    end
  end

  @spec subscribe(pid(), pid(), term(), binary(), binary() | nil) :: :ok | {:error, term()}
  def subscribe(apid, pid, sid, subject, queue) do
    GenServer.call(apid, {:subscribe, pid, sid, subject, queue})
  end

  def unsubscribe(apid, pid, sid), do: GenServer.call(apid, {:unsubscribe, pid, sid})

  @doc """
  Watch subscription interest (used by leaf/route links). The watcher receives
  `{:interest, account, subject, queue, weight}` whenever the weight of an exact
  `{subject, queue}` changes, where `weight` excludes the watcher's own
  subscriptions. Returns the current interest list in the same shape.

  With `exclude_routes: true` (route links) the weight also leaves out
  interest that any route link registered.
  """
  @spec watch_interest(name(), pid(), keyword()) ::
          {:ok, [{binary(), binary() | nil, pos_integer()}]}
  def watch_interest(name, pid, opts \\ []) do
    {:ok, apid, _} = ensure(name)
    GenServer.call(apid, {:watch_interest, pid, opts})
  end

  def unwatch_interest(name, pid) do
    case lookup(name) do
      {:ok, apid, _} -> GenServer.cast(apid, {:unwatch_interest, pid})
      :error -> :ok
    end
  end

  @doc """
  Watch *local* interest (used by gateways): like `watch_interest/2`, but the
  watcher receives `{:local_interest, account, subject, queue, weight}` where
  `weight` counts only client/in-VM subscriptions on this server (no link
  subscriptions of any kind). Returns the current local interest.
  """
  @spec watch_local_interest(name(), pid()) ::
          {:ok, [{binary(), binary() | nil, pos_integer()}]}
  def watch_local_interest(name, pid) do
    {:ok, apid, _} = ensure(name)
    GenServer.call(apid, {:watch_local_interest, pid})
  end

  def unwatch_local_interest(name, pid) do
    case lookup(name) do
      {:ok, apid, _} -> GenServer.cast(apid, {:unwatch_local_interest, pid})
      :error -> :ok
    end
  end

  def unsubscribe_all(apid, pid), do: GenServer.call(apid, {:unsubscribe_all, pid})

  @doc "Connected client pids and their metadata."
  def clients(name) do
    case lookup(name) do
      {:ok, apid, _} -> GenServer.call(apid, :clients)
      :error -> %{}
    end
  end

  def stats(name) do
    case lookup(name) do
      {:ok, apid, sublist} ->
        %{
          name: name,
          clients: map_size(GenServer.call(apid, :clients)),
          subscriptions: Sublist.count(sublist)
        }

      :error ->
        nil
    end
  end

  # ---------------------------------------------------------------- publishing

  @doc """
  Match `subject` against the account's subscriptions and deliver.

  Runs in the caller. Queue groups get exactly one (random) member each.
  `from` is excluded when `echo?` is false. Returns the number of deliveries.
  Delivered messages look like
  `{:natsbeam_msg, sid, subject, reply, headers | nil, payload}`.
  """
  @spec publish(
          Sublist.t(),
          binary(),
          binary() | nil,
          binary() | nil,
          binary(),
          pid() | nil,
          boolean()
        ) ::
          non_neg_integer()
  def publish(sublist, subject, reply, headers, payload, from \\ nil, echo? \\ true) do
    publish_matches(
      Sublist.match(sublist, subject),
      Sublist.remote_peers(sublist),
      subject,
      reply,
      headers,
      payload,
      from,
      echo?
    )
  end

  @doc """
  Like `publish/7` but with the `Sublist.match/2` result and the
  `Sublist.remote_peers/1` list supplied by the caller, so a connection can
  cache both per subject / per interest generation (see `Natsbeam.Client`)
  instead of walking the trie and reading ETS for every message.
  """
  @spec publish_matches(
          [Sublist.match()],
          [pid()],
          binary(),
          binary() | nil,
          binary() | nil,
          binary(),
          pid() | nil,
          boolean()
        ) :: non_neg_integer()
  def publish_matches([], [], _subject, _reply, _headers, _payload, _from, _echo?), do: 0

  def publish_matches([], peers, subject, reply, headers, payload, _from, _echo?) do
    forward(peers, subject, reply, headers, payload, [])
  end

  def publish_matches(matches, peers, subject, reply, headers, payload, from, echo?) do
    matches = if echo?, do: matches, else: Enum.reject(matches, fn {pid, _, _} -> pid == from end)
    {n, served_queues} = deliver(matches, subject, reply, headers, payload)
    n + forward(peers, subject, reply, headers, payload, served_queues)
  end

  @doc """
  Publish a message that arrived over a route/leaf link. It is delivered to
  local subscribers and BEAM-cluster peers but never to other links (nats
  servers form a full mesh, so links must not relay). Link subscriptions are
  recognised by their `{:link, ...}` sids.
  """
  def publish_from_link(sublist, match_subject, subject, reply, headers, payload, queues \\ nil) do
    matches =
      Sublist.match(sublist, match_subject)
      |> Enum.reject(fn {_, sid, _} -> link_sid?(sid) end)
      |> Enum.filter(fn {_, _, q} ->
        is_nil(queues) or (is_nil(q) and queues == []) or q in (queues || [])
      end)

    {n, served} = deliver(matches, subject, reply, headers, payload)

    n +
      forward(Sublist.remote_peers(sublist), subject, reply, headers, payload, served, nil, :no_links)
  end

  @doc """
  Publish a message that arrived over a gateway. Like `publish_from_link/7`
  (never relayed to any link), but with nats-server's gateway semantics for
  the queue list of an `RMSG`: plain subscribers always receive the message,
  queue groups only when named in `queues` (`nil` = every group, used for
  `_GR_.` routed replies). BEAM peers apply the same filter.
  """
  def publish_from_gateway(sublist, match_subject, subject, reply, headers, payload, queues) do
    matches =
      Sublist.match(sublist, match_subject)
      |> Enum.reject(fn {_, sid, _} -> link_sid?(sid) end)
      |> Enum.filter(fn {_, _, q} -> is_nil(q) or is_nil(queues) or q in queues end)

    {n, served} =
      if match_subject == subject,
        do: deliver(matches, subject, reply, headers, payload),
        else: deliver_routed(matches, match_subject, subject, reply, headers, payload)

    route = if match_subject != subject, do: match_subject

    n +
      forward(
        Sublist.remote_peers(sublist),
        subject,
        reply,
        headers,
        payload,
        served,
        route,
        {:no_links, queues}
      )
  end

  @doc false
  def link_sid?({:link, _}), do: true
  def link_sid?({:route_link, _}), do: true
  def link_sid?(_), do: false

  @doc false
  defdelegate gateway_sid?(sid), to: Sublist

  @doc """
  Route a message by `deliver_to` but present it to subscribers with `subject`
  in the frame. This is how JetStream deliveries work: a push consumer's
  deliver subject (or a pull request's inbox) selects the receivers, while the
  message keeps the subject it was stored under.
  """
  @spec publish_routed(Sublist.t(), binary(), binary(), binary() | nil, binary() | nil, binary()) ::
          non_neg_integer()
  def publish_routed(sublist, deliver_to, subject, reply, headers, payload) do
    matches = Sublist.match(sublist, deliver_to)
    {n, served} = deliver_routed(matches, deliver_to, subject, reply, headers, payload)
    n + forward(Sublist.remote_peers(sublist), subject, reply, headers, payload, served, deliver_to)
  end

  # Links (routes/leafs to nats-server) need the deliver subject too: nats
  # encodes it as `$JS.ACK...@<deliver>` on the reply, or uses the deliver
  # subject itself when there is no reply. Local subscribers get the plain form.
  @doc false
  def deliver_routed(
        matches,
        deliver_to,
        subject,
        reply,
        headers,
        payload,
        skip \\ fn _ -> false end
      ) do
    {links, locals} = Enum.split_with(matches, fn {_, sid, _} -> link_sid?(sid) end)
    {n1, served1} = deliver(locals, subject, reply, headers, payload, skip)

    # nats-server convention: frame subject is the delivery target, and a
    # `$JS.ACK` reply carries the original subject after '@'.
    lreply =
      if is_binary(reply) and String.starts_with?(reply, "$JS.ACK."),
        do: reply <> "@" <> subject,
        else: reply

    lsubject = deliver_to

    {n2, served2} = deliver(links, lsubject, lreply, headers, payload, skip)
    {n1 + n2, served1 ++ served2}
  end

  @doc false
  # Returns {deliveries, queue_names_served}.
  def deliver(matches, subject, reply, headers, payload, skip_queue? \\ fn _ -> false end)

  def deliver([], _subject, _reply, _headers, _payload, _skip_queue?), do: {0, []}

  def deliver(matches, subject, reply, headers, payload, skip_queue?) do
    msg = {:natsbeam_msg, nil, subject, reply, headers, payload}

    # One pass over the matches: plain subscribers (sent as we go), gateway
    # plain interest, and queue groups (collected, one member chosen below).
    {n_plain, links, gw_plain, queues} = partition(matches, msg, 0, [], [], %{})

    # A route/leaf link gets one copy no matter how many of its remote
    # subscriptions overlap; the peer fans out locally. Links are rare enough
    # that they are collected and de-duplicated separately from client subs.
    n_links =
      case links do
        [] ->
          0

        _ ->
          uniq = links |> Enum.reverse() |> Enum.uniq_by(fn {pid, _, _} -> pid end)
          Enum.each(uniq, fn {pid, sid, _} -> send(pid, put_elem(msg, 1, sid)) end)
          length(uniq)
      end

    {n_chosen, served, gw_chosen} =
      case queues do
        q when map_size(q) == 0 ->
          {0, [], []}

        _ ->
          Enum.reduce(queues, {0, [], []}, fn {q, members}, {n, served, gws} = acc ->
            if skip_queue?.(q) do
              acc
            else
              {pid, sid, _} = member = pick_queue_member(members)

              if gateway_sid?(sid) do
                {n, [q | served], [member | gws]}
              else
                send(pid, put_elem(msg, 1, sid))
                {n + 1, [q | served], gws}
              end
            end
          end)
      end

    # Gateways get exactly one frame per message carrying everything that
    # matched for them (plain interest, queue groups, mapped replies): a
    # nats-server gateway RMSG delivers to plain subscribers *and* the listed
    # queue groups, so separate frames would duplicate plain deliveries.
    n_gw =
      case {gw_plain, gw_chosen} do
        {[], []} ->
          0

        _ ->
          targets =
            Enum.group_by(gw_plain ++ gw_chosen, fn {pid, _, _} -> pid end, fn {_, {:link, key}, _} ->
              key
            end)

          Enum.each(targets, fn {pid, keys} ->
            send(pid, put_elem(msg, 1, {:link, {:gw_batch, keys}}))
          end)

          map_size(targets)
      end

    {n_plain + n_links + n_chosen + n_gw, served}
  end

  # Plain client subscriptions are sent immediately; everything that needs a
  # second look (links, gateways, queue groups) is accumulated.
  defp partition([], _msg, n, links, gw, queues), do: {n, links, gw, queues}

  defp partition([{pid, sid, nil} = m | rest], msg, n, links, gw, queues) do
    case sid do
      {:link, {:gw, _, _}} ->
        partition(rest, msg, n, links, [m | gw], queues)

      {:link, _} ->
        partition(rest, msg, n, [m | links], gw, queues)

      {:route_link, _} ->
        partition(rest, msg, n, [m | links], gw, queues)

      _ ->
        send(pid, put_elem(msg, 1, sid))
        partition(rest, msg, n + 1, links, gw, queues)
    end
  end

  defp partition([{_, _, q} = m | rest], msg, n, links, gw, queues) do
    partition(rest, msg, n, links, gw, Map.update(queues, q, [m], &[m | &1]))
  end

  # nats-server only sends a queue message to another cluster when no member
  # of the group exists in its own cluster, so gateway members come last.
  defp pick_queue_member([single]), do: single

  defp pick_queue_member(members) do
    case Enum.reject(members, fn {_, sid, _} -> gateway_sid?(sid) end) do
      [] -> Enum.random(members)
      locals -> Enum.random(locals)
    end
  end

  # BEAM clustering: every account process joins a `:pg` group named after the
  # account. Publishes are forwarded to the same account on other nodes, which
  # deliver to their local subscribers (and do not forward again).
  #
  # Queue groups get one delivery cluster-wide: groups served on the publishing
  # node are skipped remotely, and each remaining group is owned by exactly one
  # remote node, picked by hashing the queue name over the sorted remote list.
  #
  # The sorted remote list lives in the sublist's meta table; the account
  # process keeps it current from `:pg.monitor/2` notifications, so publishers
  # never consult `:pg` themselves (clients cache it per interest generation).
  defp forward(
         peers,
         subject,
         reply,
         headers,
         payload,
         served_queues,
         route \\ nil,
         flag \\ nil
       )

  defp forward([], _, _, _, _, _, _, _), do: 0

  defp forward(sorted, subject, reply, headers, payload, served_queues, route, flag) do
    count = length(sorted)

    sorted
    |> Enum.with_index()
    |> Enum.each(fn {pid, idx} ->
      send(
        pid,
        {:natsbeam_remote_publish, subject, reply, headers, payload, served_queues, idx, count,
         route, flag}
      )
    end)

    count
  end

  # Subscribe to membership changes of this account's `:pg` group and publish
  # the current remote members to the sublist. Returns the monitor ref (or nil
  # when the pg scope is not running, e.g. clustering disabled in tests).
  defp watch_remote_peers(name, sublist) do
    {ref, members} = :pg.monitor(Natsbeam.PG, {:account, name})
    Sublist.set_remote_peers(sublist, remote_sorted(members))
    ref
  catch
    :exit, _ -> nil
  end

  defp remote_sorted(members) do
    members |> Enum.reject(&(node(&1) == node())) |> Enum.sort()
  end

  defp refresh_remote_peers(%{name: name, sublist: sublist}) do
    members = :pg.get_members(Natsbeam.PG, {:account, name})
    Sublist.set_remote_peers(sublist, remote_sorted(members))
  catch
    :exit, _ -> :ok
  end

  # ---------------------------------------------------------------- callbacks

  @impl true
  def init(name) do
    Process.flag(:trap_exit, true)
    sublist = Sublist.new(name)
    {_, _} = Registry.update_value(Natsbeam.Registry, {:account, name}, fn _ -> sublist end)
    limits = Natsbeam.Config.get([:accounts, name]) || %{}
    if Natsbeam.Config.get(:cluster, true), do: :pg.join(Natsbeam.PG, {:account, name}, self())
    pg_ref = watch_remote_peers(name, sublist)
    Natsbeam.JetStream.on_account_init(name, sublist)
    Logger.debug("natsbeam: account #{name} started")
    :telemetry.execute([:natsbeam, :account, :start], %{}, %{account: name})
    {:ok, %__MODULE__{name: name, sublist: sublist, limits: limits, pg_ref: pg_ref}}
  end

  @impl true
  def handle_call({:join, pid, meta}, _from, state) do
    max = Map.get(state.limits, :max_connections) || account_jwt_conn_limit(state.name)

    cond do
      Map.has_key?(state.clients, pid) ->
        {:reply, :ok, state}

      is_integer(max) and max >= 0 and map_size(state.clients) >= max ->
        {:reply, {:error, :max_connections}, state}

      true ->
        ref = Process.monitor(pid)

        {:reply, :ok,
         %{
           state
           | clients: Map.put(state.clients, pid, meta),
             monitors: Map.put(state.monitors, ref, pid)
         }}
    end
  end

  def handle_call({:subscribe, pid, sid, subject, queue}, _from, state) do
    max = Map.get(state.limits, :max_subscriptions)

    if is_integer(max) and max > 0 and Sublist.count(state.sublist) >= max do
      {:reply, {:error, :max_subscriptions}, state}
    else
      state = ensure_monitor(state, pid)
      Sublist.insert(state.sublist, pid, sid, subject, queue)
      notify_interest(state, [{subject, queue}])
      {:reply, :ok, state}
    end
  end

  def handle_call({:unsubscribe, pid, sid}, _from, state) do
    affected =
      for {^sid, subject, queue} <- Sublist.subscriptions(state.sublist, pid),
          do: {subject, queue}

    result = Sublist.remove(state.sublist, pid, sid)
    notify_interest(state, affected)
    {:reply, result, state}
  end

  def handle_call({:unsubscribe_all, pid}, _from, state) do
    affected =
      for {_, subject, queue} <- Sublist.subscriptions(state.sublist, pid), do: {subject, queue}

    result = Sublist.remove_all(state.sublist, pid)
    notify_interest(state, affected)
    {:reply, result, state}
  end

  def handle_call({:watch_interest, pid}, from, state),
    do: handle_call({:watch_interest, pid, []}, from, state)

  def handle_call({:watch_interest, pid, opts}, _from, state) do
    if old = state.watchers[pid], do: Process.demonitor(elem(old, 0), [:flush])
    ref = Process.monitor(pid)
    exclude = watcher_exclude(pid, Keyword.get(opts, :exclude_routes, false))
    state = %{state | watchers: Map.put(state.watchers, pid, {ref, exclude})}
    {:reply, {:ok, Sublist.interest(state.sublist, exclude)}, state}
  end

  def handle_call({:watch_local_interest, pid}, _from, state) do
    ref = Process.monitor(pid)
    state = %{state | local_watchers: Map.put(state.local_watchers, pid, ref)}
    {:reply, {:ok, Sublist.local_interest(state.sublist)}, state}
  end

  def handle_call(:clients, _from, state), do: {:reply, state.clients, state}

  def handle_call(:shutdown, _from, state) do
    for {pid, _} <- state.clients, do: send(pid, {:natsbeam_close, "Account Removed"})
    {:stop, :normal, :ok, state}
  end

  @impl true
  def handle_cast({:adopt, pid}, state), do: {:noreply, ensure_monitor(state, pid)}

  def handle_cast({:unwatch_interest, pid}, state) do
    if w = state.watchers[pid], do: Process.demonitor(elem(w, 0), [:flush])
    {:noreply, %{state | watchers: Map.delete(state.watchers, pid)}}
  end

  def handle_cast({:unwatch_local_interest, pid}, state) do
    if ref = state.local_watchers[pid], do: Process.demonitor(ref, [:flush])
    {:noreply, %{state | local_watchers: Map.delete(state.local_watchers, pid)}}
  end

  def handle_cast({:leave, pid}, state) do
    {:noreply, %{state | clients: Map.delete(state.clients, pid)}}
  end

  @impl true
  def handle_info(
        {:natsbeam_remote_publish, subject, reply, headers, payload, served, idx, count, route,
         flag},
        state
      ) do
    matches = Sublist.match(state.sublist, route || subject)

    matches =
      case flag do
        :no_links ->
          Enum.reject(matches, fn {_, sid, _} -> link_sid?(sid) end)

        # From a gateway: no links, and only the queue groups the RMSG named.
        {:no_links, qf} ->
          matches
          |> Enum.reject(fn {_, sid, _} -> link_sid?(sid) end)
          |> Enum.filter(fn {_, _, q} -> is_nil(q) or is_nil(qf) or q in qf end)

        _ ->
          matches
      end

    skip? = fn q -> q in served or rem(:erlang.phash2(q), count) != idx end

    if route,
      do: deliver_routed(matches, route, subject, reply, headers, payload, skip?),
      else: deliver(matches, subject, reply, headers, payload, skip?)

    {:noreply, state}
  end

  # `:pg.monitor/2` notifications: the account's group gained or lost members.
  def handle_info({ref, event, {:account, _}, _pids}, %{pg_ref: ref} = state)
      when event in [:join, :leave] do
    refresh_remote_peers(state)
    {:noreply, state}
  end

  def handle_info({:DOWN, ref, :process, pid, _reason}, state) do
    affected =
      for {_, subject, queue} <- Sublist.subscriptions(state.sublist, pid), do: {subject, queue}

    Sublist.remove_all(state.sublist, pid)

    state = %{
      state
      | watchers: Map.delete(state.watchers, pid),
        local_watchers: Map.delete(state.local_watchers, pid)
    }

    notify_interest(state, affected)

    {:noreply,
     %{state | clients: Map.delete(state.clients, pid), monitors: Map.delete(state.monitors, ref)}}
  end

  def handle_info(_, state), do: {:noreply, state}

  @impl true
  def terminate(_reason, state) do
    :telemetry.execute([:natsbeam, :account, :stop], %{}, %{account: state.name})
    Sublist.delete(state.sublist)
    :ok
  end

  # Tell every watcher the new weight of each affected {subject, queue}, from its own point of view.
  defp notify_interest(%{watchers: w, local_watchers: lw}, _affected) when w == %{} and lw == %{},
    do: :ok

  defp notify_interest(state, affected) do
    for {subject, queue} <- Enum.uniq(affected), {wpid, {_, exclude}} <- state.watchers do
      send(
        wpid,
        {:interest, state.name, subject, queue,
         Sublist.weight(state.sublist, subject, queue, exclude)}
      )
    end

    if state.local_watchers != %{} do
      for {subject, queue} <- affected do
        w = Sublist.local_weight(state.sublist, subject, queue)

        for {wpid, _} <- state.local_watchers,
            do: send(wpid, {:local_interest, state.name, subject, queue, w})
      end
    end

    :ok
  end

  # A watcher never counts its own subscriptions; route watchers also skip
  # interest learned over any route (nats-server never relays route interest
  # to another route, and with route pooling one peer has several connections).
  defp watcher_exclude(pid, false), do: pid

  defp watcher_exclude(pid, true) do
    fn
      {^pid, _} -> true
      {_, {:route_link, _}} -> true
      _ -> false
    end
  end

  defp ensure_monitor(state, pid) do
    if Map.has_key?(state.clients, pid) or pid in Map.values(state.monitors) do
      state
    else
      ref = Process.monitor(pid)
      %{state | monitors: Map.put(state.monitors, ref, pid)}
    end
  end

  defp account_jwt_conn_limit(name) do
    case Natsbeam.Config.get([:auth, :account_jwts, name]) do
      %{"nats" => %{"limits" => %{"conn" => conn}}} when is_integer(conn) and conn >= 0 -> conn
      _ -> nil
    end
  end
end
