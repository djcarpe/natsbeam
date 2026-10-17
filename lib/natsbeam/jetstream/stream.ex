defmodule Natsbeam.JetStream.Stream do
  @moduledoc """
  One process per JetStream stream *replica*; one of them is the leader.

  The leader subscribes (in-VM) to the stream's subjects, stores matching
  messages, answers publishers with PubAcks, enforces limits/retention and
  per-message TTLs, serves get/delete/purge/snapshot for the API, runs the
  stream's consumers (on its node), pulls from mirror/source origins, and
  replicates every store mutation to the replicas. Replicas apply the log,
  keep consumer checkpoints, and elect a new leader when the leader dies.

  Replication is leader-driven with majority acknowledgement: a PubAck is only
  sent once enough replicas have applied the message.
  """
  use GenServer, restart: :transient
  require Logger

  alias Natsbeam.{Local, Subject}
  alias Natsbeam.JetStream
  alias Natsbeam.JetStream.{Cluster, Consumer, Store, Util}
  alias Natsbeam.Protocol.Headers

  defstruct account: nil,
            name: nil,
            key: nil,
            config: %{},
            store: nil,
            created: nil,
            dir: nil,
            role: :replica,
            leader: nil,
            leader_ref: nil,
            replicas: %{},
            waiting_acks: [],
            sids: [],
            consumers: %{},
            consumer_meta: %{},
            consumer_states: %{},
            dedupe: %{},
            last_msg_id: nil,
            ttl: nil,
            never: nil,
            sources: %{},
            elect_attempt: 0,
            stepdown_until: nil,
            timers: %{}

  @two_minutes_ns 120_000_000_000
  @default_dup_window_ns @two_minutes_ns
  @source_batch 256

  # ----------------------------------------------------------------- naming

  def key(account, name), do: Cluster.stream_key(account, name)

  def replica_via(account, name),
    do: {:via, Registry, {Natsbeam.Registry, {:stream_replica, account, name}}}

  @doc "Find the leader of a stream anywhere in the cluster."
  def lookup(account, name) do
    case Cluster.whereis(key(account, name)) do
      nil -> {:error, :stream_not_found}
      pid -> {:ok, pid}
    end
  end

  @doc "Stream names in an account, cluster-wide."
  def list(account), do: account |> Cluster.stream_names() |> Enum.sort()

  @doc "Local replica process for a stream (nil when this node holds none)."
  def local_replica(account, name) do
    case Registry.lookup(Natsbeam.Registry, {:stream_replica, account, name}) do
      [{pid, _}] -> pid
      [] -> nil
    end
  end

  def start_link({account, config, dir, created, opts}) do
    case Keyword.get(opts, :role, :leader) do
      :leader ->
        case GenServer.start_link(__MODULE__, {account, config, dir, created, opts},
               name: {:global, key(account, config["name"])}
             ) do
          # Another node leads (e.g. we were restarted after losing an
          # election): come back as a replica of that leader instead.
          {:error, {:already_started, leader}} when node(leader) != node() ->
            opts = opts |> Keyword.put(:role, :replica) |> Keyword.put(:leader, leader)

            GenServer.start_link(__MODULE__, {account, config, dir, created, opts},
              name: replica_via(account, config["name"])
            )

          other ->
            other
        end

      :replica ->
        GenServer.start_link(__MODULE__, {account, config, dir, created, opts},
          name: replica_via(account, config["name"])
        )
    end
  end

  # ----------------------------------------------------------------- config

  @doc "Fill defaults and validate a stream config. Returns `{:ok, config}` or `{:error, reason}`."
  def normalize_config(cfg) when is_map(cfg) do
    name = cfg["name"]
    subjects = List.wrap(cfg["subjects"] || [])
    storage = cfg["storage"] || "file"
    retention = cfg["retention"] || "limits"
    discard = cfg["discard"] || "old"
    max_age = int(cfg["max_age"], 0)
    mirror = normalize_source(cfg["mirror"])
    sources = cfg["sources"] |> List.wrap() |> Enum.map(&normalize_source/1)

    dup =
      case int(cfg["duplicate_window"], 0) do
        0 when max_age > 0 and max_age < @default_dup_window_ns -> max_age
        0 -> @default_dup_window_ns
        n -> n
      end

    normalized =
      %{
        "name" => name,
        "description" => cfg["description"],
        "subjects" => subjects,
        "retention" => retention,
        "max_consumers" => unlimited(int(cfg["max_consumers"], -1)),
        "max_msgs" => unlimited(int(cfg["max_msgs"], -1)),
        "max_bytes" => unlimited(int(cfg["max_bytes"], -1)),
        "max_age" => max_age,
        "max_msgs_per_subject" => unlimited(int(cfg["max_msgs_per_subject"], -1)),
        "max_msg_size" => unlimited(int(cfg["max_msg_size"], -1)),
        "discard" => discard,
        "storage" => storage,
        "num_replicas" => max(int(cfg["num_replicas"], 1), 1),
        "duplicate_window" => dup,
        "compression" => cfg["compression"] || "none",
        "allow_direct" => cfg["allow_direct"] == true,
        "mirror_direct" => cfg["mirror_direct"] == true,
        "sealed" => cfg["sealed"] == true,
        "deny_delete" => cfg["deny_delete"] == true,
        "deny_purge" => cfg["deny_purge"] == true,
        "allow_rollup_hdrs" => cfg["allow_rollup_hdrs"] == true,
        "discard_new_per_subject" => cfg["discard_new_per_subject"] == true,
        "allow_msg_ttl" => cfg["allow_msg_ttl"] == true,
        "subject_delete_marker_ttl" => int(cfg["subject_delete_marker_ttl"], 0),
        "consumer_limits" => cfg["consumer_limits"] || %{},
        "metadata" => cfg["metadata"],
        "republish" => cfg["republish"],
        "subject_transform" => cfg["subject_transform"],
        "placement" => cfg["placement"],
        "mirror" => mirror,
        "sources" => if(sources == [], do: nil, else: sources),
        "first_seq" => cfg["first_seq"]
      }
      |> Enum.reject(fn {k, v} -> is_nil(v) or (k == "subject_delete_marker_ttl" and v == 0) end)
      |> Map.new()

    cond do
      not Util.valid_name?(name) ->
        {:error,
         {:stream_invalid_config,
          "stream name is required and may not contain spaces, dots or wildcards"}}

      mirror && subjects != [] ->
        {:error, :stream_mirror_with_subjects}

      mirror && sources != [] ->
        {:error, :stream_mirror_with_sources}

      Enum.any?(
        [mirror | sources],
        &(&1 && &1["external"] && not is_binary(&1["external"]["api"]))
      ) ->
        {:error, {:stream_invalid_config, "external source needs an api prefix"}}

      Enum.any?([mirror | sources], &(&1 && &1["name"] == name)) ->
        {:error, {:stream_invalid_config, "stream can not source from itself"}}

      is_nil(mirror) and sources == [] and subjects == [] ->
        {:error, {:stream_invalid_config, "stream needs at least one configured subject"}}

      not Enum.all?(subjects, &Subject.valid_subscription?/1) ->
        {:error, {:stream_invalid_config, "invalid subject"}}

      storage not in ["file", "memory"] ->
        {:error, {:stream_invalid_config, "unknown storage type"}}

      retention not in ["limits", "interest", "workqueue"] ->
        {:error, {:stream_invalid_config, "unknown retention policy"}}

      discard not in ["old", "new"] ->
        {:error, {:stream_invalid_config, "unknown discard policy"}}

      normalized["num_replicas"] > 5 ->
        {:error, {:stream_invalid_config, "replicas > 5 not supported"}}

      normalized["discard_new_per_subject"] and
          (discard != "new" or normalized["max_msgs_per_subject"] <= 0) ->
        {:error,
         {:stream_invalid_config,
          "discard new per subject requires discard new policy and max messages per subject"}}

      normalized["sealed"] ->
        {:error, {:stream_invalid_config, "stream can not be sealed at creation"}}

      normalized["subject_delete_marker_ttl"] && not normalized["allow_msg_ttl"] ->
        {:error, {:stream_invalid_config, "subject delete marker TTL requires allow_msg_ttl"}}

      true ->
        {:ok, normalized}
    end
  end

  defp normalize_source(nil), do: nil

  defp normalize_source(src) when is_map(src) do
    %{
      "name" => src["name"],
      "opt_start_seq" => src["opt_start_seq"],
      "opt_start_time" => src["opt_start_time"],
      "filter_subject" => src["filter_subject"],
      "subject_transforms" => src["subject_transforms"],
      "external" => src["external"]
    }
    |> Enum.reject(fn {_, v} -> is_nil(v) end)
    |> Map.new()
  end

  # nats-server treats 0 as "unlimited" for these and reports -1.
  defp unlimited(n) when is_integer(n) and n <= 0, do: -1
  defp unlimited(n), do: n

  defp int(nil, default), do: default
  defp int(v, _default) when is_integer(v), do: v
  defp int(v, default) when is_float(v), do: if(v == trunc(v), do: trunc(v), else: default)
  defp int(_, default), do: default

  # -------------------------------------------------------------------- api

  def info(pid, opts \\ %{}), do: GenServer.call(pid, {:info, opts})
  def config(pid), do: GenServer.call(pid, :config)
  def update(pid, config), do: GenServer.call(pid, {:update, config})
  def get_msg(pid, req), do: GenServer.call(pid, {:get_msg, req})
  def delete_msg(pid, seq, no_erase), do: GenServer.call(pid, {:delete_msg, seq, no_erase})
  def purge(pid, req), do: GenServer.call(pid, {:purge, req})
  def delete(pid), do: GenServer.call(pid, :delete)
  def usage(pid), do: GenServer.call(pid, :usage)
  def stepdown(pid), do: GenServer.call(pid, :stepdown)
  def remove_peer(pid, server_name), do: GenServer.call(pid, {:remove_peer, server_name})
  def snapshot(pid, no_consumers?), do: GenServer.call(pid, {:snapshot, no_consumers?}, 60_000)
  def load_snapshot(pid, archive), do: GenServer.call(pid, {:load_snapshot, archive}, 60_000)

  def read_after(pid, seq, filter, limit),
    do: GenServer.call(pid, {:read_after, seq, filter, limit})

  def first_seq_at(pid, ts), do: GenServer.call(pid, {:first_seq_at, ts})

  def attach_consumer(pid, name, cpid, filters, cfg \\ nil, created \\ nil),
    do: GenServer.call(pid, {:attach_consumer, name, cpid, filters, cfg, created})

  def detach_consumer(pid, name, how \\ :leadership),
    do: GenServer.cast(pid, {:detach_consumer, name, how})

  def consumer_checkpoint(pid, name, data),
    do: GenServer.cast(pid, {:consumer_checkpoint, name, data})

  def acked(pid, consumer, seq), do: GenServer.cast(pid, {:acked, consumer, seq})

  # --------------------------------------------------------------- callbacks

  @impl true
  def init({account, config, dir, created, opts}) do
    Process.flag(:trap_exit, true)
    name = config["name"]
    store_dir = if config["storage"] == "file", do: dir
    store = Store.open(store_dir)
    Cluster.join_members(account, name)

    state = %__MODULE__{
      account: account,
      name: name,
      key: key(account, name),
      config: config,
      store: store,
      created: created || Util.now_ns(),
      dir: dir,
      ttl: :ets.new(:js_ttl, [:ordered_set, :private]),
      never: :ets.new(:js_never, [:set, :private])
    }

    state = rebuild_ttl_index(state) |> load_consumer_meta_from_disk()
    save_meta(state)
    state = state |> schedule(:age, 1_000) |> schedule(:sync, 2_000) |> schedule(:dedupe, 10_000)

    state =
      case Keyword.get(opts, :role, :leader) do
        :leader ->
          Logger.debug(
            "natsbeam: jetstream stream #{account}/#{name} leader (#{store.msgs} msgs)"
          )

          become_leader(state, Keyword.get(opts, :recovered, false))

        :replica ->
          Logger.debug(
            "natsbeam: jetstream stream #{account}/#{name} replica (#{store.msgs} msgs)"
          )

          attach_leader(
            %{state | role: :replica},
            Keyword.get(opts, :leader) || Cluster.whereis(state.key)
          )
      end

    {:ok, state}
  end

  # -- read-only calls work on any role

  @impl true
  def handle_call({:info, opts}, _from, state), do: {:reply, build_info(state, opts), state}
  def handle_call(:config, _from, state), do: {:reply, state.config, state}
  def handle_call(:by_subject_tab, _from, state), do: {:reply, state.store.by_subject, state}

  def handle_call(:usage, _from, state),
    do:
      {:reply,
       %{bytes: state.store.bytes, msgs: state.store.msgs, storage: state.config["storage"]},
       state}

  def handle_call(:repl_state, _from, state) do
    {:reply,
     %{
       last_seq: state.store.last_seq,
       msgs: state.store.msgs,
       node: node(),
       role: state.role,
       eligible: eligible?(state)
     }, state}
  end

  def handle_call({:read_after, seq, filter, limit}, _from, state) do
    {:reply, {:ok, Store.read_after(state.store, seq, filter, limit), state.store.last_seq},
     state}
  end

  def handle_call({:first_seq_at, ts}, _from, state) do
    seq =
      state.store.tab
      |> :ets.select([{{:"$1", :_, :_, :_, :"$2"}, [{:>=, :"$2", ts}], [:"$1"]}])
      |> Enum.min(fn -> state.store.last_seq + 1 end)

    {:reply, seq, state}
  end

  def handle_call({:get_msg, req}, _from, state) do
    store = state.store

    result =
      cond do
        is_binary(req["last_by_subj"]) ->
          Store.last_by_subject(store, req["last_by_subj"])

        is_binary(req["next_by_subj"]) ->
          Store.next_by_subject(store, req["next_by_subj"], max(req["seq"] || 1, 1))

        is_integer(req["seq"]) ->
          Store.get(store, req["seq"])

        true ->
          :bad
      end

    case result do
      :bad -> {:reply, {:error, :bad_request}, state}
      nil -> {:reply, {:error, :no_message_found}, state}
      msg -> {:reply, {:ok, msg}, state}
    end
  end

  # -- mutating calls need the leader

  def handle_call(_msg, _from, %{role: :replica} = state) do
    {:reply, {:error, :cluster_not_leader}, state}
  end

  def handle_call({:update, new_cfg}, _from, state) do
    old = state.config

    cond do
      new_cfg["storage"] != old["storage"] ->
        {:reply,
         {:error,
          {:stream_update_err, "stream configuration update can not change storage type"}}, state}

      new_cfg["retention"] != old["retention"] ->
        {:reply,
         {:error,
          {:stream_update_err, "stream configuration update can not change retention policy"}},
         state}

      old["sealed"] and not new_cfg["sealed"] ->
        {:reply,
         {:error,
          {:stream_update_err, "stream configuration update can not unseal a sealed stream"}},
         state}

      new_cfg["mirror"] != old["mirror"] ->
        {:reply, {:error, :stream_mirror_not_updatable}, state}

      true ->
        state =
          if new_cfg["subjects"] != old["subjects"] do
            state |> unsubscribe_subjects() |> Map.put(:config, new_cfg) |> subscribe_subjects()
          else
            %{state | config: new_cfg}
          end

        state = state |> enforce_limits() |> ensure_replicas() |> sync_sources()
        save_meta(state)
        replicate(state, {:cfg, new_cfg})
        {:reply, {:ok, build_info(state, %{})}, state}
    end
  end

  def handle_call({:delete_msg, seq, _no_erase}, _from, state) do
    cond do
      state.config["deny_delete"] ->
        {:reply, {:error, :stream_msg_delete_not_allowed}, state}

      state.config["sealed"] ->
        {:reply, {:error, :stream_sealed}, state}

      true ->
        case delete_seq(state, seq, "Remove") do
          {true, state} -> {:reply, :ok, state}
          {false, state} -> {:reply, {:error, {:sequence_not_found, seq}}, state}
        end
    end
  end

  def handle_call({:purge, req}, _from, state) do
    cond do
      state.config["deny_purge"] ->
        {:reply, {:error, :stream_purge_not_allowed}, state}

      state.config["sealed"] ->
        {:reply, {:error, :stream_sealed}, state}

      true ->
        opts = [filter: req["filter"], seq: req["seq"], keep: req["keep"]]
        {n, state} = purge_store(state, opts)
        {:reply, {:ok, n}, state}
    end
  end

  def handle_call(:delete, _from, state) do
    for {_, {cpid, _}} <- state.consumers, do: Consumer.stream_deleted(cpid)
    state = unsubscribe_subjects(state)
    replicate(state, :deleted)
    Cluster.unregister(state.key)
    Store.destroy(state.store)
    if state.dir, do: File.rm_rf(state.dir)
    {:stop, :normal, :ok, %{state | store: nil, dir: nil}}
  end

  def handle_call({:attach_consumer, name, cpid, filters, cfg, created}, _from, state) do
    max = state.config["max_consumers"]

    if is_integer(max) and max >= 0 and map_size(state.consumers) >= max and
         not Map.has_key?(state.consumers, name) do
      {:reply, {:error, :maximum_consumers_limit}, state}
    else
      Process.monitor(cpid)
      consumers = Map.put(state.consumers, name, {cpid, filters})
      state = %{state | consumers: consumers}

      state =
        if cfg && is_binary(cfg["durable_name"]) do
          meta = %{"config" => cfg, "created" => created}
          replicate(state, {:cmeta, name, meta})
          %{state | consumer_meta: Map.put(state.consumer_meta, name, meta)}
        else
          state
        end

      {:reply,
       {:ok,
        %{tab: state.store.tab, last_seq: state.store.last_seq, first_seq: state.store.first_seq}},
       state}
    end
  end

  def handle_call(:stepdown, _from, state) do
    if map_size(state.replicas) == 0 do
      {:reply, {:error, {:bad_request, "stream has no replicas to step down to"}}, state}
    else
      {:reply, :ok, do_stepdown(state)}
    end
  end

  def handle_call({:remove_peer, server_name}, _from, state) do
    case Enum.find(state.replicas, fn {n, _} -> Cluster.server_name(n) == server_name end) do
      nil ->
        {:reply, {:error, {:bad_request, "peer not found: #{server_name}"}}, state}

      {n, %{pid: pid}} ->
        send(pid, :stop_replica)
        state = %{state | replicas: Map.delete(state.replicas, n)}
        {:reply, :ok, ensure_replicas(state, [n])}
    end
  end

  def handle_call({:snapshot, no_consumers?}, _from, state) do
    consumers =
      if no_consumers? do
        []
      else
        for {name, {pid, _}} <- state.consumers, Map.has_key?(state.consumer_meta, name) do
          meta = state.consumer_meta[name]
          saved = safe_call(pid, :checkpoint, nil)
          %{"config" => meta["config"], "created" => meta["created"], "state" => saved}
        end
      end

    archive =
      :erlang.term_to_binary(
        %{
          version: 1,
          config: state.config,
          created: state.created,
          last_seq: state.store.last_seq,
          msgs: Store.all(state.store),
          consumers: consumers
        },
        [:compressed]
      )

    {:reply, {:ok, archive, build_info(state, %{})}, state}
  end

  def handle_call({:load_snapshot, archive}, _from, state) do
    with {:ok, %{msgs: msgs} = snap} <- decode_archive(archive) do
      store = Store.load_snapshot(state.store, msgs, snap[:last_seq] || 0)
      state = %{state | store: store} |> rebuild_ttl_index()
      replicate(state, {:snapshot, msgs, store.last_seq, %{}, %{}})

      if snap[:consumers] not in [nil, []], do: send(self(), {:start_consumers, snap[:consumers]})
      {:reply, {:ok, build_info(state, %{})}, state}
    else
      {:error, reason} -> {:reply, {:error, {:stream_restore_err, inspect(reason)}}, state}
    end
  end

  @impl true
  def handle_cast({:detach_consumer, name, how}, state) do
    state = %{state | consumers: Map.delete(state.consumers, name)}

    state =
      if how == :deleted do
        replicate(state, {:cdel, name})
        remove_consumer_files(state, name)

        %{
          state
          | consumer_meta: Map.delete(state.consumer_meta, name),
            consumer_states: Map.delete(state.consumer_states, name)
        }
      else
        state
      end

    {:noreply, state}
  end

  def handle_cast({:consumer_checkpoint, name, data}, state) do
    if Map.has_key?(state.consumer_meta, name) do
      replicate(state, {:cstate, name, data})
      {:noreply, %{state | consumer_states: Map.put(state.consumer_states, name, data)}}
    else
      {:noreply, state}
    end
  end

  def handle_cast({:acked, consumer, seq}, state) do
    {:noreply, retention_ack(state, consumer, seq)}
  end

  # -- replication (replica side)

  def handle_cast({:repl, leader, entry}, %{role: :replica} = state) do
    state = if state.leader == leader, do: apply_repl(state, entry), else: state
    {:noreply, state}
  end

  def handle_cast({:repl, _leader, _entry}, state), do: {:noreply, state}

  # -- replication (leader side)

  def handle_cast({:repl_ack, n, pid, applied}, %{role: :leader} = state) do
    replicas =
      Map.update(
        state.replicas,
        n,
        %{pid: pid, ref: Process.monitor(pid), applied: applied, active: now_ms()},
        fn r ->
          %{r | applied: max(r.applied, applied), active: now_ms()}
        end
      )

    {:noreply, release_acks(%{state | replicas: replicas})}
  end

  def handle_cast({:replica_hello, n, pid, first_seq, last_seq, msgs}, %{role: :leader} = state) do
    in_sync? = last_seq == state.store.last_seq and msgs == state.store.msgs
    applied = if in_sync?, do: last_seq, else: 0

    replicas =
      case Map.get(state.replicas, n) do
        nil ->
          Map.put(state.replicas, n, %{
            pid: pid,
            ref: Process.monitor(pid),
            applied: applied,
            active: now_ms()
          })

        r ->
          Map.put(state.replicas, n, %{
            r
            | pid: pid,
              applied: max(r.applied, applied),
              active: now_ms()
          })
      end

    state = %{state | replicas: replicas}
    sync_replica(state, pid, first_seq, last_seq, msgs)
    {:noreply, state}
  end

  def handle_cast(_, state), do: {:noreply, state}

  # -- messages

  @impl true
  def handle_info(
        {:natsbeam_msg, _sid, subject, reply, headers, payload},
        %{role: :leader} = state
      ) do
    if String.starts_with?(subject, "$JS.") or String.starts_with?(subject, "$SYS.") do
      {:noreply, state}
    else
      {:noreply, ingest(state, subject, reply, headers, payload)}
    end
  end

  def handle_info({:natsbeam_msg, _, _, _, _, _}, state), do: {:noreply, state}

  # origin stream notifications for mirrors/sources
  def handle_info({:js_msg, _seq, _subject}, state),
    do: {:noreply, schedule(state, :source_fetch, 0)}

  def handle_info({:js_deleted, seq, _subject}, state), do: {:noreply, mirror_delete(state, seq)}
  def handle_info({:js_purged, first}, state), do: {:noreply, mirror_purge(state, first)}
  def handle_info(:source_fetch, state), do: {:noreply, fetch_sources(state)}

  def handle_info({:ext_msg, origin, seq, subject, hdrs, data, ts}, %{role: :leader} = state),
    do: {:noreply, store_external(state, origin, seq, subject, hdrs, data, ts)}

  def handle_info({:ext_msg, _, _, _, _, _, _}, state), do: {:noreply, state}

  def handle_info({:ext_state, origin, first, deleted}, %{role: :leader} = state),
    do: {:noreply, external_removals(state, origin, first, deleted)}

  def handle_info({:ext_state, _, _, _}, state), do: {:noreply, state}

  def handle_info(:source_tick, state),
    do: {:noreply, state |> sync_sources() |> fetch_sources() |> schedule(:source_tick, 1_000)}

  def handle_info({:DOWN, ref, :process, pid, reason}, state) do
    cond do
      state.role == :replica and ref == state.leader_ref ->
        Logger.info(
          "natsbeam: stream #{state.account}/#{state.name} lost its leader (#{inspect(reason)})"
        )

        {:noreply,
         %{state | leader: nil, leader_ref: nil, elect_attempt: 0}
         |> schedule(:elect, 300 + :rand.uniform(200))}

      Enum.any?(state.replicas, fn {_, r} -> r.ref == ref end) ->
        {n, _} = Enum.find(state.replicas, fn {_, r} -> r.ref == ref end)
        state = %{state | replicas: Map.delete(state.replicas, n)} |> release_acks()
        {:noreply, schedule(state, :peer_check, 2_000)}

      Enum.any?(state.sources, fn {_, s} -> s.ref == ref end) ->
        sources =
          Map.new(state.sources, fn {k, s} ->
            if s.ref == ref, do: {k, %{s | pid: nil, ref: nil}}, else: {k, s}
          end)

        {:noreply, %{state | sources: sources}}

      true ->
        consumers = state.consumers |> Enum.reject(fn {_, {p, _}} -> p == pid end) |> Map.new()
        {:noreply, %{state | consumers: consumers}}
    end
  end

  def handle_info({:new_leader, pid}, %{role: :replica} = state) when pid != self() do
    {:noreply, attach_leader(state, pid)}
  end

  def handle_info({:new_leader, _}, state), do: {:noreply, state}

  def handle_info({:leader_stepdown, _old}, %{role: :replica} = state) do
    {:noreply,
     %{state | leader: nil, elect_attempt: 0} |> schedule(:elect, 100 + :rand.uniform(100))}
  end

  def handle_info({:leader_stepdown, _}, state), do: {:noreply, state}

  def handle_info(:elect, %{role: :replica, leader: nil} = state), do: {:noreply, elect(state)}
  def handle_info(:elect, state), do: {:noreply, state}

  def handle_info({:sync, entries}, %{role: :replica} = state) do
    state = Enum.reduce(entries, state, &apply_repl(&2, &1))
    {:noreply, state}
  end

  def handle_info({:snapshot, msgs, last_seq, cmeta, cstates}, %{role: :replica} = state) do
    {:noreply, apply_repl(state, {:snapshot, msgs, last_seq, cmeta, cstates})}
  end

  def handle_info(:stop_replica, %{role: :replica} = state) do
    Store.destroy(state.store)
    if state.dir, do: File.rm_rf(state.dir)
    {:stop, :normal, %{state | store: nil, dir: nil}}
  end

  def handle_info(:stop_replica, state), do: {:noreply, state}

  def handle_info(:recover_consumers, %{role: :leader} = state) do
    recover_consumers(state)
    {:noreply, state}
  end

  def handle_info({:start_consumers, consumers}, %{role: :leader} = state) do
    for c <- consumers do
      start_consumer_async(state, c["config"], c["created"], c["state"])
    end

    {:noreply, state}
  end

  def handle_info(:peer_check, %{role: :leader} = state), do: {:noreply, ensure_replicas(state)}
  def handle_info(:peer_check, state), do: {:noreply, state}

  def handle_info(:age, state) do
    state = if state.role == :leader, do: state |> expire_old() |> expire_ttl(), else: state
    {:noreply, schedule(state, :age, 1_000)}
  end

  def handle_info(:sync, state) do
    {:noreply, %{state | store: Store.sync(state.store)} |> schedule(:sync, 2_000)}
  end

  def handle_info(:dedupe, state) do
    now = Util.now_ns()
    window = state.config["duplicate_window"]
    dedupe = state.dedupe |> Enum.reject(fn {_, {_, ts}} -> now - ts > window end) |> Map.new()
    {:noreply, %{state | dedupe: dedupe} |> schedule(:dedupe, 10_000)}
  end

  def handle_info({:ack_timeout, seq}, state), do: {:noreply, timeout_acks(state, seq)}
  def handle_info(_, state), do: {:noreply, state}

  @impl true
  def terminate(_reason, %{store: nil}), do: :ok

  def terminate(_reason, state) do
    Store.close(state.store)
    :ok
  end

  # ------------------------------------------------------------- leadership

  defp become_leader(state, recovered?) do
    state =
      %{state | role: :leader, leader: nil, leader_ref: nil, elect_attempt: 0}
      |> subscribe_subjects()
      |> schedule(:peer_check, 500)
      |> schedule(:source_tick, 200)

    # Replicas that already exist (after a failover) need to learn about us.
    for pid <- Cluster.members(state.account, state.name),
        pid != self(),
        do: send(pid, {:new_leader, self()})

    :telemetry.execute([:natsbeam, :jetstream, :leader], %{}, %{
      account: state.account,
      stream: state.name
    })

    state = ensure_replicas(state)
    # Consumers call back into us, so start them once init/this callback is over.
    if recovered? or map_size(state.consumer_meta) > 0, do: send(self(), :recover_consumers)
    state
  end

  defp attach_leader(state, nil),
    do: schedule(%{state | leader: nil}, :elect, 300 + :rand.uniform(200))

  defp attach_leader(state, leader) do
    if state.leader_ref, do: Process.demonitor(state.leader_ref, [:flush])
    ref = Process.monitor(leader)
    store = state.store

    GenServer.cast(
      leader,
      {:replica_hello, node(), self(), store.first_seq, store.last_seq, store.msgs}
    )

    %{state | leader: leader, leader_ref: ref, role: :replica}
  end

  # Deterministic election: the member with the most data wins; ties go to the
  # lowest node name. Everyone else waits for the winner to register.
  defp elect(state) do
    case Cluster.whereis(state.key) do
      pid when is_pid(pid) and pid != self() ->
        attach_leader(state, pid)

      _ ->
        others = Cluster.members(state.account, state.name) -- [self()]

        others_state =
          for pid <- others,
              (st = safe_call(pid, :repl_state, nil)) != nil,
              st.role == :replica,
              st.eligible do
            {pid, st}
          end

        mine = %{last_seq: state.store.last_seq, msgs: state.store.msgs, node: node()}
        me = if eligible?(state), do: [{self(), mine}], else: []

        ranked =
          Enum.sort_by(me ++ others_state, fn {_, s} ->
            {-s.last_seq, -s.msgs, s.node}
          end)

        case ranked do
          [{pid, _} | _] when pid == self() ->
            case Cluster.register(state.key) do
              :yes ->
                Logger.info(
                  "natsbeam: stream #{state.account}/#{state.name}: #{node()} elected leader"
                )

                become_leader(state, true)

              :no ->
                attach_leader(state, Cluster.whereis(state.key))
            end

          _ ->
            schedule(
              %{state | elect_attempt: state.elect_attempt + 1},
              :elect,
              300 + :rand.uniform(300)
            )
        end
    end
  end

  defp do_stepdown(state) do
    Logger.info("natsbeam: stream #{state.account}/#{state.name}: leader stepping down")
    Cluster.unregister(state.key)
    for {_, {cpid, _}} <- state.consumers, do: Consumer.leadership_lost(cpid)
    state = unsubscribe_subjects(state) |> detach_sources()
    for {_, r} <- state.replicas, do: send(r.pid, {:leader_stepdown, self()})

    for {_, r} <- state.replicas, do: Process.demonitor(r.ref, [:flush])

    %{
      state
      | role: :replica,
        replicas: %{},
        consumers: %{},
        waiting_acks: [],
        leader: nil,
        stepdown_until: now_ms() + 10_000
    }
    |> schedule(:elect, 1_500)
  end

  defp recover_consumers(state) do
    for {name, meta} <- state.consumer_meta, not Map.has_key?(state.consumers, name) do
      saved = Map.get(state.consumer_states, name)
      start_consumer_async(state, meta["config"], meta["created"], saved)
    end

    :ok
  end

  # Consumers call back into the stream during init, so start them from a task.
  defp start_consumer_async(state, cfg, created, saved) do
    {account, stream, storage} = {state.account, state.name, state.config["storage"]}

    Task.Supervisor.start_child(Natsbeam.JetStream.TaskSupervisor, fn ->
      JetStream.start_consumer_local(account, stream, cfg, created, saved, storage)
    end)
  end

  # ------------------------------------------------------------- replicas

  # Make sure num_replicas - 1 peers hold a replica, starting new ones on free nodes.
  defp ensure_replicas(state, exclude \\ [])

  defp ensure_replicas(%{role: :leader} = state, exclude) do
    wanted = state.config["num_replicas"] - 1
    have = Map.keys(state.replicas)

    cond do
      wanted > length(have) ->
        candidates = Cluster.pick_peers(wanted - length(have), [node() | have] ++ exclude)

        Enum.reduce(candidates, state, fn n, acc ->
          case :rpc.call(
                 n,
                 JetStream,
                 :start_replica,
                 [acc.account, acc.config, acc.created, self()],
                 5_000
               ) do
            {:ok, pid} ->
              %{
                acc
                | replicas:
                    Map.put(acc.replicas, n, %{
                      pid: pid,
                      ref: Process.monitor(pid),
                      applied: 0,
                      active: now_ms()
                    })
              }

            {:error, {:already_started, pid}} ->
              %{
                acc
                | replicas:
                    Map.put(acc.replicas, n, %{
                      pid: pid,
                      ref: Process.monitor(pid),
                      applied: 0,
                      active: now_ms()
                    })
              }

            other ->
              Logger.warning(
                "natsbeam: could not place replica of #{acc.name} on #{n}: #{inspect(other)}"
              )

              acc
          end
        end)

      wanted < length(have) ->
        {drop, _} = Enum.split(Enum.sort(have), length(have) - wanted)

        Enum.reduce(drop, state, fn n, acc ->
          r = acc.replicas[n]
          Process.demonitor(r.ref, [:flush])
          send(r.pid, :stop_replica)
          %{acc | replicas: Map.delete(acc.replicas, n)}
        end)

      true ->
        state
    end
  end

  defp ensure_replicas(state, _), do: state

  defp replicate(%{replicas: replicas}, _entry) when replicas == %{}, do: :ok

  defp replicate(state, entry) do
    for {_, r} <- state.replicas, do: GenServer.cast(r.pid, {:repl, self(), entry})
    :ok
  end

  # Decide what a (re)connecting replica needs: nothing, the tail, or everything.
  defp sync_replica(state, pid, _r_first, r_last, r_msgs) do
    store = state.store

    cond do
      r_last == store.last_seq and r_msgs == store.msgs ->
        :ok

      r_last < store.last_seq and r_last >= store.first_seq - 1 and
          r_msgs == count_upto(store, r_last) ->
        entries =
          store
          |> Store.read_after(r_last, nil, :infinity)
          |> Enum.map(fn {seq, subject, hdrs, data, ts} -> {:m, seq, subject, hdrs, data, ts} end)

        entries = entries ++ [{:l, store.last_seq}]
        send(pid, {:sync, entries})

      true ->
        send(
          pid,
          {:snapshot, Store.all(store), store.last_seq, state.consumer_meta,
           state.consumer_states}
        )
    end
  end

  defp count_upto(store, seq) do
    :ets.select_count(store.tab, [{{:"$1", :_, :_, :_, :_}, [{:"=<", :"$1", seq}], [true]}])
  end

  defp apply_repl(state, {:m, seq, subject, hdrs, data, ts}) do
    case Store.append_at(state.store, seq, subject, hdrs, data, ts) do
      {:ok, store} ->
        state = %{state | store: store} |> index_ttl(seq, hdrs, ts)
        ack_leader(state)

      {:error, _} ->
        state
    end
  end

  defp apply_repl(state, {:d, seq}) do
    {_, store} = Store.delete(state.store, seq)
    %{state | store: store}
  end

  defp apply_repl(state, {:l, seq}) do
    %{state | store: Store.bump_last_seq(state.store, seq)} |> ack_leader()
  end

  defp apply_repl(state, {:p, opts, last_seq}) do
    {_, store} = Store.purge(state.store, opts)
    store = Store.bump_last_seq(store, last_seq)
    %{state | store: store}
  end

  defp apply_repl(state, {:cfg, cfg}) do
    state = %{state | config: cfg}
    save_meta(state)
    state
  end

  defp apply_repl(state, {:cmeta, name, meta}) do
    write_consumer_file(state, name, "meta.json", meta)
    %{state | consumer_meta: Map.put(state.consumer_meta, name, meta)}
  end

  defp apply_repl(state, {:cstate, name, data}) do
    write_consumer_file(state, name, "state.json", data)
    %{state | consumer_states: Map.put(state.consumer_states, name, data)}
  end

  defp apply_repl(state, {:cdel, name}) do
    remove_consumer_files(state, name)

    %{
      state
      | consumer_meta: Map.delete(state.consumer_meta, name),
        consumer_states: Map.delete(state.consumer_states, name)
    }
  end

  defp apply_repl(state, {:snapshot, msgs, last_seq, cmeta, cstates}) do
    store = Store.load_snapshot(state.store, msgs, last_seq)
    state = %{state | store: store} |> rebuild_ttl_index()
    for {name, meta} <- cmeta, do: write_consumer_file(state, name, "meta.json", meta)
    for {name, data} <- cstates, do: write_consumer_file(state, name, "state.json", data)

    state = %{
      state
      | consumer_meta: Map.merge(state.consumer_meta, cmeta),
        consumer_states: Map.merge(state.consumer_states, cstates)
    }

    ack_leader(state)
  end

  defp apply_repl(state, :deleted) do
    Store.destroy(state.store)
    if state.dir, do: File.rm_rf(state.dir)
    Process.send_after(self(), :stop_replica, 0)
    %{state | store: Store.open(nil), dir: nil}
  end

  defp apply_repl(state, _), do: state

  defp ack_leader(%{leader: nil} = state), do: state

  defp ack_leader(state) do
    GenServer.cast(state.leader, {:repl_ack, node(), self(), state.store.last_seq})
    state
  end

  # PubAcks wait until a majority (leader + needed replicas) has the message.
  defp needed_acks(state), do: div(state.config["num_replicas"], 2)

  defp quorum_seq(state) do
    needed = needed_acks(state)

    if needed == 0 do
      state.store.last_seq
    else
      applied = state.replicas |> Map.values() |> Enum.map(& &1.applied) |> Enum.sort(:desc)
      Enum.at(applied, needed - 1, 0)
    end
  end

  defp release_acks(%{waiting_acks: []} = state), do: state

  defp release_acks(state) do
    q = quorum_seq(state)
    {ready, waiting} = Enum.split_with(state.waiting_acks, fn {seq, _, _} -> seq <= q end)
    for {_, reply, ack} <- ready, do: pub_ack(state, reply, ack)
    %{state | waiting_acks: waiting}
  end

  defp queue_ack(state, seq, reply, ack) do
    if needed_acks(state) == 0 or map_size(state.replicas) < needed_acks(state) do
      # No quorum possible (R1, or replicas missing): ack from the leader alone.
      pub_ack(state, reply, ack)
      state
    else
      Process.send_after(self(), {:ack_timeout, seq}, 5_000)
      %{state | waiting_acks: state.waiting_acks ++ [{seq, reply, ack}]}
    end
  end

  defp timeout_acks(state, seq) do
    {late, rest} = Enum.split_with(state.waiting_acks, fn {s, _, _} -> s <= seq end)
    for {_, reply, ack} <- late, do: pub_ack(state, reply, ack)
    %{state | waiting_acks: rest}
  end

  # ----------------------------------------------------------------- ingest

  defp ingest(state, subject, reply, headers, payload) do
    hdrs = if headers, do: decode_headers(headers), else: %{}
    msg_id = hdrs["nats-msg-id"]

    with :ok <- check_sealed(state),
         :ok <- check_mirror(state),
         nil <- duplicate(state, msg_id),
         :ok <- check_expected(state, subject, hdrs),
         :ok <- check_size(state, headers, payload),
         {:ok, ttl} <- check_ttl(state, hdrs),
         {:ok, state} <- rollup(state, subject, hdrs),
         {:ok, state} <- make_room(state, subject) do
      {seq, state} = store_message(state, subject, headers, payload, Util.now_ns(), ttl)
      state = %{state | last_msg_id: msg_id || state.last_msg_id}

      state =
        if msg_id,
          do: %{state | dedupe: Map.put(state.dedupe, msg_id, {seq, Util.now_ns()})},
          else: state

      state = retention_on_store(state, seq, subject)
      queue_ack(state, seq, reply, %{"stream" => state.name, "seq" => seq})
    else
      {:duplicate, seq} ->
        pub_ack(state, reply, %{"stream" => state.name, "seq" => seq, "duplicate" => true})
        state

      {:error, err} ->
        pub_ack(state, reply, %{"error" => err_map(err)})
        state
    end
  end

  # Common store path for published, sourced, mirrored and marker messages.
  defp store_message(state, subject, headers, payload, ts, ttl, at \\ nil) do
    {seq, store} =
      case at do
        nil ->
          Store.append(state.store, subject, headers, payload, ts)

        seq ->
          {:ok, store} = Store.append_at(state.store, seq, subject, headers, payload, ts)
          {seq, store}
      end

    state = %{state | store: store} |> remember_ttl(seq, ttl, ts) |> enforce_limits()
    replicate(state, {:m, seq, subject, headers, payload, ts})
    notify_consumers(state, {:js_msg, seq, subject})
    {seq, state}
  end

  defp err_map({key, detail}), do: Util.error(key, detail)
  defp err_map(key) when is_atom(key), do: Util.error(key)
  defp err_map(%{} = m), do: m

  defp decode_headers(block) do
    case Headers.decode(block) do
      {:ok, %{fields: fields}} -> Map.new(fields, fn {k, v} -> {String.downcase(k), v} end)
      :error -> %{}
    end
  end

  defp check_sealed(%{config: %{"sealed" => true}}), do: {:error, :stream_sealed}
  defp check_sealed(_), do: :ok

  defp check_mirror(%{config: %{"mirror" => m}}) when is_map(m),
    do: {:error, {:stream_store_failed, "mirrors can not be published to directly"}}

  defp check_mirror(_), do: :ok

  defp duplicate(_state, nil), do: nil

  defp duplicate(state, msg_id) do
    case Map.get(state.dedupe, msg_id) do
      {seq, _} -> {:duplicate, seq}
      nil -> nil
    end
  end

  defp check_expected(state, subject, hdrs) do
    store = state.store

    cond do
      hdrs["nats-expected-stream"] && hdrs["nats-expected-stream"] != state.name ->
        {:error, :stream_not_match}

      hdrs["nats-expected-last-sequence"] &&
          to_int(hdrs["nats-expected-last-sequence"]) != store.last_seq ->
        {:error, {:stream_wrong_last_sequence, store.last_seq}}

      hdrs["nats-expected-last-subject-sequence"] ->
        last =
          case :ets.lookup(store.by_subject, subject) do
            [{_, _, l}] -> l
            [] -> 0
          end

        if to_int(hdrs["nats-expected-last-subject-sequence"]) == last,
          do: :ok,
          else: {:error, {:stream_wrong_last_sequence, last}}

      hdrs["nats-expected-last-msg-id"] && hdrs["nats-expected-last-msg-id"] != state.last_msg_id ->
        {:error, {:stream_wrong_last_msg_id, state.last_msg_id || ""}}

      true ->
        :ok
    end
  end

  defp to_int(s) do
    case Integer.parse(String.trim(s)) do
      {n, _} -> n
      _ -> -1
    end
  end

  defp check_size(%{config: %{"max_msg_size" => max}}, headers, payload)
       when is_integer(max) and max >= 0 do
    if byte_size(payload) + byte_size(headers || "") > max,
      do: {:error, :stream_msg_exceeds_max},
      else: :ok
  end

  defp check_size(_, _, _), do: :ok

  defp check_ttl(state, hdrs) do
    allowed? = state.config["allow_msg_ttl"] == true

    case hdrs["nats-ttl"] do
      nil ->
        {:ok, nil}

      _ when not allowed? ->
        {:error, :message_ttl_disabled}

      value ->
        case Util.parse_ttl(value) do
          {:ok, ttl} -> {:ok, ttl}
          :error -> {:error, :message_ttl_invalid}
        end
    end
  end

  defp rollup(state, subject, hdrs) do
    allowed? = state.config["allow_rollup_hdrs"] == true

    case hdrs["nats-rollup"] do
      nil ->
        {:ok, state}

      _ when not allowed? ->
        {:error, {:stream_store_failed, "rollup not permitted"}}

      "sub" ->
        {_, state} = purge_store(state, filter: subject)
        {:ok, state}

      "all" ->
        {_, state} = purge_store(state, [])
        {:ok, state}

      _ ->
        {:error, {:stream_store_failed, "rollup value invalid: expected sub or all"}}
    end
  end

  # Apply max_msgs / max_bytes / max_msgs_per_subject before storing one more message.
  defp make_room(state, subject) do
    cfg = state.config
    store = state.store
    discard_new? = cfg["discard"] == "new"
    per_subj = cfg["max_msgs_per_subject"]

    cond do
      cfg["max_msgs"] > 0 and store.msgs >= cfg["max_msgs"] and discard_new? ->
        {:error, {:stream_store_failed, "maximum messages exceeded"}}

      cfg["max_bytes"] > 0 and store.bytes >= cfg["max_bytes"] and discard_new? ->
        {:error, {:stream_store_failed, "maximum bytes exceeded"}}

      per_subj > 0 and Store.count_for(store, subject) >= per_subj and discard_new? and
          cfg["discard_new_per_subject"] ->
        {:error, {:stream_store_failed, "maximum messages per subject exceeded"}}

      true ->
        state =
          if per_subj > 0 and Store.count_for(store, subject) >= per_subj do
            drop_oldest_for(state, subject, Store.count_for(store, subject) - per_subj + 1)
          else
            state
          end

        {:ok, enforce_limits(state)}
    end
  end

  defp drop_oldest_for(state, _subject, n) when n <= 0, do: state

  defp drop_oldest_for(state, subject, n) do
    case Store.first_seq_for(state.store, subject) do
      nil ->
        state

      seq ->
        {_, state} = delete_seq(state, seq, nil)
        drop_oldest_for(state, subject, n - 1)
    end
  end

  # Trim oldest messages until max_msgs / max_bytes hold (discard old).
  defp enforce_limits(state) do
    cfg = state.config
    store = state.store

    over? =
      (cfg["max_msgs"] > 0 and store.msgs > cfg["max_msgs"]) or
        (cfg["max_bytes"] > 0 and store.bytes > cfg["max_bytes"])

    if over? and store.msgs > 0 do
      {_, state} = delete_seq(state, store.first_seq, "Remove")
      enforce_limits(state)
    else
      state
    end
  end

  # -- max_age

  defp expire_old(%{config: %{"max_age" => age}} = state) when is_integer(age) and age > 0 do
    cutoff = Util.now_ns() - age
    expire_before(state, cutoff, state.store.first_seq)
  end

  defp expire_old(state), do: state

  defp expire_before(state, cutoff, seq) do
    store = state.store

    case Store.get(store, seq) do
      {^seq, _, _, _, ts} when ts < cutoff ->
        if :ets.member(state.never, seq) do
          # TTL "never": exempt from max_age, keep scanning past it.
          case :ets.next(store.tab, seq) do
            :"$end_of_table" -> state
            next -> expire_before(state, cutoff, next)
          end
        else
          {_, state} = delete_seq(state, seq, "MaxAge")
          expire_before(state, cutoff, state.store.first_seq)
        end

      _ ->
        state
    end
  end

  # -- per-message TTL

  defp remember_ttl(state, _seq, nil, _ts), do: state

  defp remember_ttl(state, seq, :never, _ts) do
    :ets.insert(state.never, {seq})
    state
  end

  defp remember_ttl(state, seq, ttl_ns, ts) do
    :ets.insert(state.ttl, {{ts + ttl_ns, seq}})
    state
  end

  # Replicas and recovered stores learn TTLs from the headers.
  defp index_ttl(state, seq, hdrs, ts) when is_binary(hdrs) do
    if String.contains?(hdrs, "Nats-TTL") do
      case Util.parse_ttl(decode_headers(hdrs)["nats-ttl"] || "") do
        {:ok, ttl} -> remember_ttl(state, seq, ttl, ts)
        :error -> state
      end
    else
      state
    end
  end

  defp index_ttl(state, _, _, _), do: state

  defp rebuild_ttl_index(state) do
    :ets.delete_all_objects(state.ttl)
    :ets.delete_all_objects(state.never)

    :ets.foldl(
      fn {seq, _, hdrs, _, ts}, acc -> index_ttl(acc, seq, hdrs, ts) end,
      state,
      state.store.tab
    )
  end

  defp expire_ttl(state) do
    now = Util.now_ns()

    case :ets.first(state.ttl) do
      {expires, seq} when expires <= now ->
        :ets.delete(state.ttl, {expires, seq})
        {_, state} = delete_seq(state, seq, "MaxAge")
        expire_ttl(state)

      _ ->
        state
    end
  end

  # When the last message of a subject goes away through a limit, leave a
  # delete marker behind (itself a TTL'd message) if the stream asks for it.
  defp maybe_marker(state, subject, reason) do
    marker_ttl = state.config["subject_delete_marker_ttl"]

    if is_integer(marker_ttl) and marker_ttl > 0 and reason != nil and
         Store.count_for(state.store, subject) == 0 do
      secs = max(div(marker_ttl, 1_000_000_000), 1)
      hdrs = Headers.encode([{"Nats-Marker-Reason", reason}, {"Nats-TTL", "#{secs}s"}])
      {_, state} = store_message(state, subject, hdrs, "", Util.now_ns(), secs * 1_000_000_000)
      state
    else
      state
    end
  end

  # --------------------------------------------------------------- retention

  # Interest retention with nobody interested: the message is acknowledged but
  # not kept, exactly like nats-server.
  defp retention_on_store(%{config: %{"retention" => "interest"}} = state, seq, subject) do
    interested =
      Enum.any?(state.consumers, fn {_, {_, filters}} -> Store.matches_any?(filters, subject) end)

    if interested, do: state, else: elem(delete_seq(state, seq, nil), 1)
  end

  defp retention_on_store(state, _seq, _subject), do: state

  defp retention_ack(%{config: %{"retention" => "workqueue"}} = state, _consumer, seq) do
    elem(delete_seq(state, seq, nil), 1)
  end

  defp retention_ack(%{config: %{"retention" => "interest"}} = state, consumer, seq) do
    case Store.get(state.store, seq) do
      nil ->
        state

      {_, subject, _, _, _} ->
        others =
          for {name, {pid, filters}} <- state.consumers,
              name != consumer,
              Store.matches_any?(filters, subject),
              do: pid

        if Enum.all?(others, &Consumer.acked?(&1, seq)),
          do: elem(delete_seq(state, seq, nil), 1),
          else: state
    end
  end

  defp retention_ack(state, _, _), do: state

  # ------------------------------------------------------- mirrors/sources

  defp source_specs(%{config: cfg}) do
    mirror = if cfg["mirror"], do: [Map.put(cfg["mirror"], "mirror", true)], else: []
    mirror ++ (cfg["sources"] || [])
  end

  # (Re)connect source workers to their origin leaders.
  defp sync_sources(%{role: :leader} = state) do
    specs = source_specs(state)

    sources =
      Enum.reduce(specs, state.sources, fn spec, acc ->
        origin = spec["name"]

        src =
          Map.get(acc, origin) ||
            %{
              name: origin,
              mirror?: spec["mirror"] == true,
              filter: spec["filter_subject"],
              last: initial_source_seq(state, spec),
              pid: nil,
              ref: nil,
              origin_last: 0,
              active: 0
            }

        src = Map.put_new(src, :external, spec["external"])

        src =
          if src.pid == nil and src.external do
            start = if is_integer(src.last), do: src.last, else: 0

            {:ok, pid} =
              Natsbeam.JetStream.ExternalSource.start_link(%{
                owner: self(),
                account: state.account,
                api: src.external["api"],
                origin: origin,
                filter: src.filter,
                last: start,
                mirror?: src.mirror?
              })

            %{src | pid: pid, ref: Process.monitor(pid), last: start}
          else
            src
          end

        src =
          if src.pid == nil do
            case lookup(state.account, origin) do
              {:ok, pid} when pid != self() ->
                filters = if src.filter, do: [src.filter], else: []

                _ =
                  safe_call(
                    pid,
                    {:attach_consumer, "$src:#{state.name}", self(), filters, nil, nil},
                    nil
                  )

                src = %{src | pid: pid, ref: Process.monitor(pid)}

                src =
                  if src.last == :by_time do
                    ts = Util.parse_ts(spec["opt_start_time"])
                    %{src | last: max((safe_call(pid, {:first_seq_at, ts}, 1) || 1) - 1, 0)}
                  else
                    src
                  end

                src

              _ ->
                src
            end
          else
            src
          end

        Map.put(acc, origin, src)
      end)

    state = %{state | sources: sources}
    if sources != %{}, do: schedule(state, :source_fetch, 0), else: state
  end

  defp sync_sources(state), do: state

  defp detach_sources(state) do
    for {_, s} <- state.sources, s.ref, do: Process.demonitor(s.ref, [:flush])
    for {_, s} <- state.sources, s[:external] && s.pid, do: GenServer.stop(s.pid, :normal)
    %{state | sources: %{}}
  end

  # Where to start pulling from the origin on (re)start.
  defp initial_source_seq(state, spec) do
    cond do
      is_integer(spec["opt_start_seq"]) and spec["opt_start_seq"] > 0 ->
        max(resume_point(state, spec), spec["opt_start_seq"] - 1)

      is_binary(spec["opt_start_time"]) and resume_point(state, spec) == 0 ->
        :by_time

      true ->
        resume_point(state, spec)
    end
  end

  # Mirrors share the origin's sequences; sources remember them in a header.
  defp resume_point(state, spec) do
    if spec["mirror"] == true do
      state.store.last_seq
    else
      origin = spec["name"]

      :ets.foldl(
        fn
          {_, _, hdrs, _, _}, acc when is_binary(hdrs) ->
            case Regex.run(~r/Nats-Stream-Source: (\S+) (\d+)/, hdrs) do
              [_, ^origin, seq] -> max(acc, String.to_integer(seq))
              _ -> acc
            end

          _, acc ->
            acc
        end,
        0,
        state.store.tab
      )
    end
  end

  defp fetch_sources(%{role: :leader, sources: sources} = state) when sources != %{} do
    Enum.reduce(sources, state, fn {origin, src}, acc ->
      if src.pid && is_integer(src.last) && is_nil(src[:external]),
        do: fetch_source(acc, origin, src),
        else: acc
    end)
  end

  defp fetch_sources(state), do: state

  # A message pulled by an ExternalSource worker (origin behind an API prefix).
  defp store_external(state, origin, seq, subject, hdrs, data, ts) do
    case Map.get(state.sources, origin) do
      nil ->
        state

      src ->
        acc =
          if src.mirror? do
            # Skip what the origin already reported removed (a delivery that
            # was in flight when the origin deleted or purged it).
            if seq > state.store.last_seq and seq >= Map.get(src, :origin_first, 0) and
                 not MapSet.member?(Map.get(src, :origin_deleted, MapSet.new()), seq) do
              {_, st} = store_message(state, subject, hdrs, data, ts, ttl_from(hdrs), seq)
              st
            else
              state
            end
          else
            hdrs =
              add_header_block(hdrs, "Nats-Stream-Source", "#{origin} #{seq} #{src.filter || ""}")

            {_, st} = store_message(state, subject, hdrs, data, ts, ttl_from(hdrs))
            st
          end

        src = %{
          src
          | last: max(seq, if(is_integer(src.last), do: src.last, else: 0)),
            origin_last: seq,
            active: Util.now_ns()
        }

        %{acc | sources: Map.put(acc.sources, origin, src)}
    end
  end

  # The origin's STREAM.INFO as seen by an ExternalSource worker (mirrors
  # only): purge locally below the origin's first_seq and delete the interior
  # sequences it reports deleted. Only positive reports of removal are acted
  # on; a local gap or a message still in flight is never treated as deleted.
  defp external_removals(
         %{config: %{"mirror" => %{"name" => origin}}} = state,
         origin,
         first,
         deleted
       ) do
    src = Map.get(state.sources, origin)

    if src do
      state =
        if state.store.msgs > 0 and state.store.first_seq < first do
          {_, st} = purge_store(state, seq: first)
          st
        else
          state
        end

      # Keep first_seq aligned with the origin's when everything is gone.
      state =
        if state.store.msgs == 0 and first - 1 > state.store.last_seq do
          replicate(state, {:l, first - 1})
          %{state | store: Store.bump_last_seq(state.store, first - 1)}
        else
          state
        end

      state =
        Enum.reduce(deleted || [], state, fn seq, acc ->
          if seq >= acc.store.first_seq and seq <= acc.store.last_seq,
            do: mirror_delete(acc, seq),
            else: acc
        end)

      origin_deleted =
        case deleted do
          nil -> Map.get(src, :origin_deleted, MapSet.new())
          list -> list |> Enum.filter(&(&1 >= first)) |> MapSet.new()
        end

      src = Map.merge(src, %{origin_first: first, origin_deleted: origin_deleted})
      %{state | sources: Map.put(state.sources, origin, src)}
    else
      state
    end
  end

  defp external_removals(state, _, _, _), do: state

  defp fetch_source(state, origin, src) do
    case safe_call(src.pid, {:read_after, src.last, src.filter, @source_batch}, nil) do
      {:ok, msgs, origin_last} ->
        {state, last} =
          Enum.reduce(msgs, {state, src.last}, fn {seq, subject, hdrs, data, ts}, {acc, _} ->
            acc =
              if src.mirror? do
                if seq > acc.store.last_seq do
                  {_, acc} = store_message(acc, subject, hdrs, data, ts, ttl_from(hdrs), seq)
                  acc
                else
                  acc
                end
              else
                hdrs =
                  add_header_block(
                    hdrs,
                    "Nats-Stream-Source",
                    "#{origin} #{seq} #{src.filter || ""}"
                  )

                {_, acc} = store_message(acc, subject, hdrs, data, ts, ttl_from(hdrs))
                acc
              end

            {acc, seq}
          end)

        # Keep a mirror's last_seq aligned with the origin even across gaps.
        state =
          if src.mirror? and origin_last > state.store.last_seq and msgs == [] do
            store = Store.bump_last_seq(state.store, origin_last)
            replicate(state, {:l, origin_last})
            %{state | store: store}
          else
            state
          end

        src = %{src | last: max(last, src.last), origin_last: origin_last, active: Util.now_ns()}
        state = %{state | sources: Map.put(state.sources, origin, src)}

        if length(msgs) == @source_batch, do: schedule(state, :source_fetch, 0), else: state

      _ ->
        state
    end
  end

  defp ttl_from(hdrs) when is_binary(hdrs) do
    case Util.parse_ttl(decode_headers(hdrs)["nats-ttl"] || "") do
      {:ok, ttl} -> ttl
      :error -> nil
    end
  end

  defp ttl_from(_), do: nil

  defp add_header_block(nil, k, v), do: Headers.encode([{k, v}])

  defp add_header_block(block, k, v),
    do: String.replace_suffix(block, "\r\n", "") <> k <> ": " <> v <> "\r\n\r\n"

  defp mirror_delete(%{config: %{"mirror" => m}, role: :leader} = state, seq) when is_map(m) do
    elem(delete_seq(state, seq, nil), 1)
  end

  defp mirror_delete(state, _seq), do: state

  defp mirror_purge(%{config: %{"mirror" => m}, role: :leader} = state, first) when is_map(m) do
    {_, state} = purge_store(state, seq: first)
    state
  end

  defp mirror_purge(state, _), do: state

  # ---------------------------------------------------------------- helpers

  defp subscribe_subjects(state) do
    sids =
      for subject <- state.config["subjects"] || [] do
        {:ok, sid} = Local.subscribe(subject, account: state.account, pid: self())
        sid
      end

    %{state | sids: sids}
  end

  defp unsubscribe_subjects(state) do
    for sid <- state.sids, do: Local.unsubscribe(sid, account: state.account)
    %{state | sids: []}
  end

  defp delete_seq(state, seq, reason) do
    case Store.get(state.store, seq) do
      nil ->
        {false, state}

      {_, subject, hdrs, _, _} ->
        {_, store} = Store.delete(state.store, seq)
        :ets.delete(state.never, seq)
        state = %{state | store: store}
        replicate(state, {:d, seq})
        notify_consumers(state, {:js_deleted, seq, subject})
        # Markers never beget markers.
        reason =
          if is_binary(hdrs) and String.contains?(hdrs, "Nats-Marker-Reason"),
            do: nil,
            else: reason

        {true, maybe_marker(state, subject, reason)}
    end
  end

  defp purge_store(state, opts) do
    {n, store} = Store.purge(state.store, opts)
    state = %{state | store: store}
    replicate(state, {:p, opts, store.last_seq})
    notify_consumers(state, {:js_purged, store.first_seq})
    {n, state}
  end

  defp notify_consumers(state, msg) do
    for {_, {pid, _}} <- state.consumers, do: send(pid, msg)
    :ok
  end

  defp pub_ack(_state, nil, _ack), do: :ok

  defp pub_ack(state, reply, %{"error" => _} = ack) do
    Local.publish(
      reply,
      Jason.encode!(Map.put(ack, "type", "io.nats.jetstream.api.v1.pub_ack_response")),
      account: state.account
    )
  end

  defp pub_ack(state, reply, ack) do
    ack = if d = JetStream.domain(), do: Map.put(ack, "domain", d), else: ack
    Local.publish(reply, Jason.encode!(ack), account: state.account)
  end

  defp safe_call(pid, msg, default, timeout \\ 5_000) do
    GenServer.call(pid, msg, timeout)
  catch
    :exit, _ -> default
  end

  defp schedule(state, key, ms) do
    if t = state.timers[key], do: Process.cancel_timer(t)
    %{state | timers: Map.put(state.timers, key, Process.send_after(self(), key, ms))}
  end

  defp now_ms, do: System.monotonic_time(:millisecond)

  defp eligible?(%{stepdown_until: nil}), do: true
  defp eligible?(%{stepdown_until: until}), do: now_ms() > until

  defp decode_archive(archive) do
    case :erlang.binary_to_term(archive, [:safe]) do
      %{version: 1, msgs: msgs} = snap when is_list(msgs) -> {:ok, snap}
      _ -> {:error, :bad_archive}
    end
  rescue
    _ -> {:error, :bad_archive}
  end

  @doc false
  def build_info(state, opts) do
    store = state.store
    first_ts = Store.first_ts(store)

    st = %{
      "messages" => store.msgs,
      "bytes" => store.bytes,
      "first_seq" => store.first_seq,
      "first_ts" => if(first_ts, do: Util.ts(first_ts), else: "0001-01-01T00:00:00Z"),
      "last_seq" => store.last_seq,
      "last_ts" =>
        if(store.last_ts && store.msgs > 0,
          do: Util.ts(store.last_ts),
          else: "0001-01-01T00:00:00Z"
        ),
      "consumer_count" => map_size(state.consumers),
      "num_subjects" => Store.num_subjects(store)
    }

    deleted = Store.deleted(store)
    st = if deleted != [], do: Map.put(st, "num_deleted", length(deleted)), else: st

    st =
      if deleted != [] and opts["deleted_details"] == true,
        do: Map.put(st, "deleted", deleted),
        else: st

    st =
      case opts["subjects_filter"] do
        f when is_binary(f) and f != "" -> Map.put(st, "subjects", Store.subject_counts(store, f))
        _ -> st
      end

    info = %{
      "config" => state.config,
      "created" => Util.ts(state.created),
      "state" => st,
      "ts" => Util.ts(Util.now_ns())
    }

    info
    |> maybe_cluster_info(state)
    |> maybe_source_info(state)
  end

  defp maybe_cluster_info(info, state) do
    if state.config["num_replicas"] > 1 or length(Cluster.nodes()) > 1 do
      leader =
        case state.role do
          :leader -> Cluster.server_name()
          :replica -> state.leader && Cluster.server_name(node(state.leader))
        end

      replicas =
        for {n, r} <- state.replicas do
          %{
            "name" => Cluster.server_name(n),
            "current" => r.applied >= state.store.last_seq,
            "active" => (now_ms() - r.active) * 1_000_000,
            "lag" => max(state.store.last_seq - r.applied, 0)
          }
        end

      Map.put(info, "cluster", %{
        "name" => Cluster.cluster_name(),
        "leader" => leader,
        "replicas" => replicas
      })
    else
      info
    end
  end

  defp maybe_source_info(info, state) do
    src_info = fn src ->
      %{
        "name" => src.name,
        "lag" => if(is_integer(src.last), do: max(src.origin_last - src.last, 0), else: 0),
        "active" => if(src.active > 0, do: Util.now_ns() - src.active, else: -1),
        "filter_subject" => src.filter
      }
      |> Enum.reject(fn {_, v} -> is_nil(v) end)
      |> Map.new()
    end

    info =
      case state.config["mirror"] do
        %{"name" => origin} ->
          src =
            state.sources[origin] ||
              %{name: origin, last: 0, origin_last: 0, active: 0, filter: nil}

          Map.put(info, "mirror", src_info.(src))

        _ ->
          info
      end

    case state.config["sources"] do
      [_ | _] = sources ->
        Map.put(
          info,
          "sources",
          Enum.map(sources, fn %{"name" => origin} = spec ->
            src =
              state.sources[origin] ||
                %{
                  name: origin,
                  last: 0,
                  origin_last: 0,
                  active: 0,
                  filter: spec["filter_subject"]
                }

            src_info.(src)
          end)
        )

      _ ->
        info
    end
  end

  # ------------------------------------------------------------ persistence

  defp save_meta(%{dir: nil}), do: :ok
  defp save_meta(%{config: %{"storage" => "memory"}}), do: :ok

  defp save_meta(state) do
    File.mkdir_p!(state.dir)

    File.write!(
      Path.join(state.dir, "meta.json"),
      Jason.encode!(%{"config" => state.config, "created" => state.created})
    )
  end

  defp consumer_dir(%{dir: nil}, _name), do: nil
  defp consumer_dir(%{config: %{"storage" => "memory"}}, _name), do: nil
  defp consumer_dir(state, name), do: Path.join([state.dir, "consumers", name])

  defp write_consumer_file(state, name, file, data) do
    case consumer_dir(state, name) do
      nil ->
        :ok

      dir ->
        File.mkdir_p!(dir)
        File.write!(Path.join(dir, file), Jason.encode!(data))
    end
  end

  defp remove_consumer_files(state, name) do
    case consumer_dir(state, name) do
      nil -> :ok
      dir -> File.rm_rf(dir)
    end
  end

  # Replicas (and recovering leaders) pick up consumer meta/state written to disk.
  defp load_consumer_meta_from_disk(%{dir: nil} = state), do: state

  defp load_consumer_meta_from_disk(state) do
    cdir = Path.join(state.dir, "consumers")

    if File.dir?(cdir) do
      Enum.reduce(File.ls!(cdir), state, fn name, acc ->
        with {:ok, body} <- File.read(Path.join([cdir, name, "meta.json"])),
             {:ok, meta} <- Jason.decode(body) do
          saved =
            case File.read(Path.join([cdir, name, "state.json"])) do
              {:ok, s} -> Jason.decode!(s)
              _ -> nil
            end

          acc = %{acc | consumer_meta: Map.put(acc.consumer_meta, name, meta)}

          if saved,
            do: %{acc | consumer_states: Map.put(acc.consumer_states, name, saved)},
            else: acc
        else
          _ -> acc
        end
      end)
    else
      state
    end
  end
end
