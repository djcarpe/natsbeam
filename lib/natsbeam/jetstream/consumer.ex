defmodule Natsbeam.JetStream.Consumer do
  @moduledoc """
  One process per JetStream consumer (pull or push).

  Reads messages straight from the stream's ETS table, delivers them with
  `$JS.ACK.<stream>.<consumer>.<delivered>.<sseq>.<cseq>.<ts>.<pending>` reply
  subjects, tracks acks/NAKs/in-progress/terms, redelivers after `ack_wait`,
  serves pull requests (`CONSUMER.MSG.NEXT`) with batch/expires/no_wait/
  heartbeats, and sends idle heartbeats for push consumers.
  """
  use GenServer, restart: :transient
  require Logger

  alias Natsbeam.{Account, Local, Subject}
  alias Natsbeam.JetStream.{Cluster, Store, Util}
  alias Natsbeam.JetStream.Stream, as: JStream
  alias Natsbeam.Protocol.Headers

  defstruct account: nil,
            stream: nil,
            stream_pid: nil,
            tab: nil,
            sublist: nil,
            name: nil,
            durable?: false,
            config: %{},
            created: nil,
            dir: nil,
            filters: nil,
            cursor: 0,
            dseq: 0,
            sseq: 0,
            ack_floor_c: 0,
            ack_floor_s: 0,
            pending: %{},
            redeliver: :gb_sets.new(),
            waiting: :queue.new(),
            num_waiting: 0,
            num_redelivered: 0,
            num_pending: 0,
            deliver_subject: nil,
            ack_wait_ms: 30_000,
            max_deliver: -1,
            max_ack_pending: 1000,
            ack_policy: "explicit",
            last_sent_ms: 0,
            last_active: nil,
            timers: %{},
            sid: nil,
            dirty: false,
            pause_until: nil

  @default_ack_wait_ns 30_000_000_000
  @default_inactive_ns 5_000_000_000

  # ----------------------------------------------------------------- naming

  def key(account, stream, name), do: Cluster.consumer_key(account, stream, name)

  @doc "Find a consumer anywhere in the cluster."
  def lookup(account, stream, name) do
    case Cluster.whereis(key(account, stream, name)) do
      nil -> {:error, :consumer_not_found}
      pid -> {:ok, pid}
    end
  end

  def list(account, stream), do: account |> Cluster.consumer_names(stream) |> Enum.sort()

  def start_link({account, stream, config, dir, created, saved}) do
    GenServer.start_link(__MODULE__, {account, stream, config, dir, created, saved},
      name: {:global, key(account, stream, config["name"])}
    )
  end

  # -------------------------------------------------------------------- api

  def info(pid), do: GenServer.call(pid, :info)
  def config(pid), do: GenServer.call(pid, :config)
  def update(pid, config), do: GenServer.call(pid, {:update, config})
  def delete(pid), do: GenServer.call(pid, :delete)
  def acked?(pid, seq), do: GenServer.call(pid, {:acked?, seq})
  def next_request(pid, reply, body), do: GenServer.cast(pid, {:next, reply, body})
  def stream_deleted(pid), do: GenServer.cast(pid, :stream_deleted)
  def leadership_lost(pid), do: GenServer.cast(pid, :leadership_lost)
  def pause(pid, until), do: GenServer.call(pid, {:pause, until})
  def checkpoint(pid), do: GenServer.call(pid, :checkpoint)

  # ----------------------------------------------------------------- config

  @doc "Fill defaults and validate a consumer config against its stream config."
  def normalize_config(stream_cfg, cfg, existing_consumers \\ []) do
    durable = cfg["durable_name"]
    name = cfg["name"] || durable || Util.random_name()
    deliver_subject = cfg["deliver_subject"]
    pull? = is_nil(deliver_subject) or deliver_subject == ""
    ack_policy = cfg["ack_policy"] || "explicit"
    filters = filters(cfg)
    workqueue? = stream_cfg["retention"] == "workqueue"

    base =
      %{
        "name" => name,
        "durable_name" => durable,
        "description" => cfg["description"],
        "deliver_policy" => cfg["deliver_policy"] || "all",
        "opt_start_seq" => cfg["opt_start_seq"],
        "opt_start_time" => cfg["opt_start_time"],
        "ack_policy" => ack_policy,
        "ack_wait" => positive(cfg["ack_wait"], @default_ack_wait_ns),
        "max_deliver" => int(cfg["max_deliver"], -1),
        "backoff" => cfg["backoff"],
        "filter_subject" => cfg["filter_subject"],
        "filter_subjects" => cfg["filter_subjects"],
        "replay_policy" => cfg["replay_policy"] || "instant",
        "rate_limit_bps" => cfg["rate_limit_bps"],
        "sample_freq" => cfg["sample_freq"],
        "max_waiting" =>
          if(pull?, do: positive(cfg["max_waiting"], 512), else: cfg["max_waiting"]),
        "max_ack_pending" =>
          if(ack_policy == "none", do: 0, else: positive(cfg["max_ack_pending"], 1000)),
        "headers_only" => cfg["headers_only"] == true,
        "max_batch" => cfg["max_batch"],
        "max_expires" => cfg["max_expires"],
        "max_bytes" => cfg["max_bytes"],
        "inactive_threshold" =>
          cfg["inactive_threshold"] || if(is_nil(durable), do: @default_inactive_ns),
        "num_replicas" => int(cfg["num_replicas"], 0),
        "mem_storage" => cfg["mem_storage"],
        "metadata" => cfg["metadata"],
        "deliver_subject" => deliver_subject,
        "deliver_group" => cfg["deliver_group"],
        "flow_control" => cfg["flow_control"],
        "idle_heartbeat" => cfg["idle_heartbeat"],
        "direct" => cfg["direct"],
        "pause_until" => cfg["pause_until"],
        "priority_groups" => cfg["priority_groups"],
        "priority_policy" => cfg["priority_policy"]
      }
      |> Enum.reject(fn {_, v} -> is_nil(v) end)
      |> Map.new()

    all_filters = filters || []

    cond do
      not Util.valid_name?(name) ->
        {:error,
         {:consumer_create_err, "consumer name may not contain spaces, dots or wildcards"}}

      durable && not Util.valid_name?(durable) ->
        {:error, {:consumer_create_err, "durable name may not contain spaces, dots or wildcards"}}

      is_binary(cfg["filter_subject"]) and is_list(cfg["filter_subjects"]) and
          cfg["filter_subjects"] != [] ->
        {:error,
         {:consumer_create_err,
          "consumer cannot have both FilterSubject and FilterSubjects specified"}}

      Enum.any?(all_filters, &(&1 == "")) ->
        {:error, :consumer_empty_filter}

      not Enum.all?(all_filters, &Subject.valid_subscription?/1) ->
        {:error, {:consumer_create_err, "invalid filter subject"}}

      (stream_cfg["subjects"] || []) != [] and
          not Enum.all?(all_filters, fn f ->
            Enum.any?(stream_cfg["subjects"], &Subject.intersects?(&1, f))
          end) ->
        {:error, :consumer_filter_not_subset}

      overlapping?(all_filters) ->
        {:error, :consumer_overlapping_filters}

      ack_policy not in ["none", "all", "explicit"] ->
        {:error, {:consumer_invalid_policy, "unknown ack policy"}}

      base["deliver_policy"] not in [
        "all",
        "last",
        "new",
        "by_start_sequence",
        "by_start_time",
        "last_per_subject"
      ] ->
        {:error, {:consumer_invalid_policy, "unknown deliver policy"}}

      base["deliver_policy"] == "by_start_sequence" and not is_integer(base["opt_start_seq"]) ->
        {:error,
         {:consumer_invalid_policy, "deliver policy by start sequence requires opt_start_seq"}}

      base["deliver_policy"] == "by_start_time" and is_nil(Util.parse_ts(base["opt_start_time"])) ->
        {:error,
         {:consumer_invalid_policy, "deliver policy by start time requires opt_start_time"}}

      pull? and ack_policy == "none" and workqueue? ->
        {:error, :consumer_pull_requires_ack}

      not pull? and not Subject.valid_publish?(deliver_subject, true) ->
        {:error, :consumer_deliver_to_wildcards}

      not pull? and is_integer(cfg["max_waiting"]) and cfg["max_waiting"] > 0 ->
        {:error, :consumer_push_max_waiting}

      workqueue? and ack_policy != "explicit" ->
        {:error, :consumer_wq_requires_explicit_ack}

      workqueue? and base["deliver_policy"] != "all" ->
        {:error, :consumer_wq_not_deliver_all}

      workqueue? and wq_conflict?(all_filters, existing_consumers, name) ->
        {:error,
         if(all_filters == [],
           do: :consumer_wq_multiple_unfiltered,
           else: :consumer_wq_not_unique
         )}

      true ->
        {:ok, base}
    end
  end

  defp filters(cfg) do
    case {cfg["filter_subject"], cfg["filter_subjects"]} do
      {f, _} when is_binary(f) and f != "" -> [f]
      {_, fs} when is_list(fs) and fs != [] -> fs
      _ -> nil
    end
  end

  defp overlapping?(filters) do
    pairs = for a <- filters, b <- filters, a != b, do: {a, b}
    Enum.any?(pairs, fn {a, b} -> Subject.intersects?(a, b) end)
  end

  # Workqueue streams: filters of distinct consumers must not overlap.
  defp wq_conflict?(filters, existing, name) do
    existing
    |> Enum.reject(fn {n, _} -> n == name end)
    |> Enum.any?(fn {_, other} ->
      other = if other == [], do: [">"], else: other
      mine = if filters == [], do: [">"], else: filters
      Enum.any?(for a <- mine, b <- other, do: Subject.intersects?(a, b))
    end)
  end

  defp int(nil, d), do: d
  defp int(v, _) when is_integer(v), do: v
  defp int(_, d), do: d
  defp positive(v, _d) when is_integer(v) and v > 0, do: v
  defp positive(_, d), do: d

  # --------------------------------------------------------------- callbacks

  @impl true
  def init({account, stream, config, dir, created, saved}) do
    Process.flag(:trap_exit, true)

    with {:ok, spid} <- JStream.lookup(account, stream),
         filters = filters(config),
         token_filters = filters && Enum.map(filters, &Subject.tokens/1),
         {:ok, %{tab: tab, last_seq: last_seq}} <-
           JStream.attach_consumer(spid, config["name"], self(), filters || [], config, created) do
      Process.monitor(spid)
      {:ok, _apid, sublist} = Account.ensure(account)

      {:ok, sid} =
        Local.subscribe("$JS.ACK.#{stream}.#{config["name"]}.>", account: account, pid: self())

      state = %__MODULE__{
        account: account,
        stream: stream,
        stream_pid: spid,
        tab: tab,
        sublist: sublist,
        name: config["name"],
        durable?: is_binary(config["durable_name"]),
        config: config,
        created: created || Util.now_ns(),
        dir: dir,
        filters: token_filters,
        sid: sid,
        last_active: Util.now_ns(),
        deliver_subject: config["deliver_subject"]
      }

      state = apply_config(state, config)
      state = if saved, do: restore(state, saved), else: start_position(state, tab, last_seq)
      state = schedule_unpause(state)
      state = %{state | num_pending: Store.count_after(tab, state.cursor, state.filters)}
      save_meta(state)

      state =
        state
        |> schedule(:ack_sweep, min(state.ack_wait_ms, 1_000))
        |> schedule(:inactive, 1_000)
        |> schedule(:save, 2_000)
        |> schedule_hb()

      {:ok, drain(state)}
    else
      {:error, reason} -> {:stop, reason}
    end
  end

  defp apply_config(state, config) do
    %{
      state
      | config: config,
        ack_wait_ms: Util.ns_to_ms(config["ack_wait"]) || 30_000,
        max_deliver: config["max_deliver"] || -1,
        max_ack_pending: config["max_ack_pending"] || 0,
        ack_policy: config["ack_policy"],
        deliver_subject: config["deliver_subject"],
        pause_until: Util.parse_ts(config["pause_until"])
    }
  end

  defp paused?(%{pause_until: nil}), do: false
  defp paused?(%{pause_until: until}), do: until > Util.now_ns()

  defp schedule_unpause(state) do
    if paused?(state) do
      ms = div(state.pause_until - Util.now_ns(), 1_000_000) + 1
      schedule(state, :unpause, ms)
    else
      state
    end
  end

  # Where to begin, per deliver_policy. `cursor` is the last stream seq considered.
  defp start_position(state, tab, last_seq) do
    case state.config["deliver_policy"] do
      "all" ->
        %{state | cursor: 0}

      "new" ->
        %{state | cursor: last_seq}

      "last" ->
        case last_matching(tab, state.filters) do
          nil -> %{state | cursor: last_seq}
          seq -> %{state | cursor: seq - 1}
        end

      "by_start_sequence" ->
        %{state | cursor: max(state.config["opt_start_seq"] - 1, 0)}

      "by_start_time" ->
        ts = Util.parse_ts(state.config["opt_start_time"])
        %{state | cursor: first_seq_at_or_after(tab, ts) - 1}

      "last_per_subject" ->
        {:ok, spid} = JStream.lookup(state.account, state.stream)
        store_struct = %Store{tab: tab, by_subject: by_subject_tab(spid)}
        seqs = Store.last_per_subject(store_struct, state.filters)
        %{state | cursor: last_seq, redeliver: :gb_sets.from_list(seqs)}
    end
  end

  # The stream exposes its by_subject table through the registry value of
  # the store struct; fetch via a call to keep it simple.
  defp by_subject_tab(spid) do
    GenServer.call(spid, :by_subject_tab)
  end

  defp last_matching(tab, filters) do
    walk_back(tab, :ets.last(tab), filters)
  end

  defp walk_back(_tab, :"$end_of_table", _filters), do: nil

  defp walk_back(tab, seq, filters) do
    case :ets.lookup(tab, seq) do
      [{_, subject, _, _, _}] ->
        if Store.matches_any?(filters, subject),
          do: seq,
          else: walk_back(tab, :ets.prev(tab, seq), filters)

      [] ->
        walk_back(tab, :ets.prev(tab, seq), filters)
    end
  end

  defp first_seq_at_or_after(tab, ts) do
    tab
    |> :ets.select([{{:"$1", :_, :_, :_, :"$2"}, [{:>=, :"$2", ts}], [:"$1"]}])
    |> Enum.min(fn ->
      :ets.last(tab) |> then(&if(&1 == :"$end_of_table", do: 1, else: &1 + 1))
    end)
  end

  @impl true
  def handle_call(:info, _from, state), do: {:reply, build_info(state), state}
  def handle_call(:config, _from, state), do: {:reply, state.config, state}

  def handle_call({:acked?, seq}, _from, state) do
    acked? =
      seq <= state.ack_floor_s or (seq <= state.cursor and not Map.has_key?(state.pending, seq))

    {:reply, acked?, state}
  end

  def handle_call(:checkpoint, _from, state), do: {:reply, checkpoint_data(state), state}

  def handle_call({:pause, until}, _from, state) do
    until_ns = Util.parse_ts(until)

    cfg =
      if until_ns && until_ns > Util.now_ns(),
        do: Map.put(state.config, "pause_until", until),
        else: Map.delete(state.config, "pause_until")

    state = apply_config(state, cfg) |> schedule_unpause()
    save_meta(state)

    JStream.attach_consumer(
      state.stream_pid,
      state.name,
      self(),
      filters(cfg) || [],
      cfg,
      state.created
    )

    paused = paused?(state)

    resp = %{
      "paused" => paused,
      "pause_until" => if(paused, do: until, else: "0001-01-01T00:00:00Z"),
      "pause_remaining" => if(paused, do: state.pause_until - Util.now_ns(), else: 0)
    }

    {:reply, {:ok, resp}, if(paused, do: state, else: drain(state))}
  end

  def handle_call({:update, new_cfg}, _from, state) do
    immutable = [
      "deliver_subject",
      "ack_policy",
      "deliver_policy",
      "durable_name",
      "name",
      "opt_start_seq",
      "opt_start_time",
      "replay_policy"
    ]

    changed = Enum.filter(immutable, fn k -> Map.get(new_cfg, k) != Map.get(state.config, k) end)

    if changed != [] do
      {:reply, {:error, {:consumer_create_err, "consumer #{hd(changed)} can not be updated"}},
       state}
    else
      filters = filters(new_cfg)

      state =
        apply_config(%{state | filters: filters && Enum.map(filters, &Subject.tokens/1)}, new_cfg)

      state = %{state | num_pending: Store.count_after(state.tab, state.cursor, state.filters)}

      JStream.attach_consumer(
        state.stream_pid,
        state.name,
        self(),
        filters || [],
        new_cfg,
        state.created
      )

      save_meta(state)
      {:reply, {:ok, build_info(state)}, schedule_unpause(state)}
    end
  end

  def handle_call(:delete, _from, state) do
    state = fail_waiting(state, "409", "Consumer Deleted")
    JStream.detach_consumer(state.stream_pid, state.name, :deleted)
    if state.dir, do: File.rm_rf(state.dir)
    {:stop, :normal, :ok, %{state | dir: nil, dirty: false}}
  end

  @impl true
  def handle_cast({:next, reply, body}, state), do: {:noreply, pull_request(state, reply, body)}

  def handle_cast(:stream_deleted, state) do
    state = fail_waiting(state, "409", "Consumer Deleted")
    if state.dir, do: File.rm_rf(state.dir)
    {:stop, :normal, %{state | dir: nil, dirty: false}}
  end

  # The stream leader moved: checkpoint and stop; the new leader recreates us.
  def handle_cast(:leadership_lost, state) do
    state = fail_waiting(state, "409", "Leadership Change")
    {:stop, :normal, state}
  end

  @impl true
  def handle_info({:js_msg, seq, subject}, state) do
    state =
      if seq > state.cursor and Store.matches_any?(state.filters, subject),
        do: %{state | num_pending: state.num_pending + 1},
        else: state

    {:noreply, drain(state)}
  end

  def handle_info({:js_deleted, seq, subject}, state) do
    state =
      cond do
        seq > state.cursor and Store.matches_any?(state.filters, subject) ->
          %{state | num_pending: max(state.num_pending - 1, 0)}

        Map.has_key?(state.pending, seq) ->
          # Message is gone; treat as settled.
          settle(state, seq, :removed)

        :gb_sets.is_member(seq, state.redeliver) ->
          %{state | redeliver: :gb_sets.delete(seq, state.redeliver)}

        true ->
          state
      end

    {:noreply, state}
  end

  def handle_info({:js_purged, first_seq}, state) do
    gone = state.pending |> Map.keys() |> Enum.filter(&(&1 < first_seq))
    state = Enum.reduce(gone, state, &settle(&2, &1, :removed))
    redeliver = :gb_sets.filter(&(&1 >= first_seq), state.redeliver)

    state = %{
      state
      | redeliver: redeliver,
        num_pending: Store.count_after(state.tab, state.cursor, state.filters)
    }

    {:noreply, state}
  end

  # Ack protocol messages on $JS.ACK.<stream>.<consumer>.<delivered>.<sseq>.<cseq>.<ts>.<pending>
  def handle_info(
        {:natsbeam_msg, _sid, "$JS.ACK." <> _ = subject, reply, _headers, payload},
        state
      ) do
    state =
      case parse_ack_subject(subject) do
        {:ok, sseq} -> handle_ack(state, sseq, payload, reply)
        :error -> state
      end

    {:noreply, state}
  end

  def handle_info({:natsbeam_msg, _, _, _, _, _}, state), do: {:noreply, state}

  def handle_info(:ack_sweep, state) do
    now = now_ms()

    expired =
      for {sseq, {_cseq, count, ts}} <- state.pending,
          now - ts >= ack_wait_for(state, count),
          not :gb_sets.is_member(sseq, state.redeliver),
          do: sseq

    state =
      Enum.reduce(expired, state, fn sseq, acc ->
        if redeliverable?(acc, sseq),
          do: %{acc | redeliver: :gb_sets.add(sseq, acc.redeliver)},
          else: acc
      end)

    {:noreply, state |> drain() |> schedule(:ack_sweep, min(state.ack_wait_ms, 1_000))}
  end

  def handle_info({:redeliver, sseq}, state) do
    state =
      if Map.has_key?(state.pending, sseq),
        do: %{state | redeliver: :gb_sets.add(sseq, state.redeliver)},
        else: state

    {:noreply, drain(state)}
  end

  def handle_info({:expire, ref}, state) do
    case take_request(state, ref) do
      {nil, state} ->
        {:noreply, state}

      {req, state} ->
        send_status(state, req.reply, "408", "Request Timeout", pending_headers(req))
        {:noreply, state}
    end
  end

  def handle_info({:request_hb, ref}, state) do
    case find_request(state, ref) do
      nil ->
        {:noreply, state}

      req ->
        send_status(state, req.reply, "100", "Idle Heartbeat", [
          {"Nats-Last-Consumer", state.dseq},
          {"Nats-Last-Stream", state.sseq}
        ])

        Process.send_after(self(), {:request_hb, ref}, req.hb_ms)
        {:noreply, state}
    end
  end

  def handle_info(:hb, state) do
    hb_ms = Util.ns_to_ms(state.config["idle_heartbeat"])

    state =
      if (state.deliver_subject && hb_ms && now_ms() - state.last_sent_ms >= hb_ms) and
           Natsbeam.Sublist.interest?(state.sublist, state.deliver_subject) do
        send_status(state, state.deliver_subject, "100", "Idle Heartbeat", [
          {"Nats-Last-Consumer", state.dseq},
          {"Nats-Last-Stream", state.sseq}
        ])

        state
      else
        state
      end

    {:noreply, schedule_hb(state)}
  end

  def handle_info(:retry, state), do: {:noreply, drain(state)}

  def handle_info(:inactive, state) do
    threshold = Util.ns_to_ms(state.config["inactive_threshold"])

    idle_ms = div(Util.now_ns() - state.last_active, 1_000_000)

    inactive? =
      not state.durable? and threshold != nil and idle_ms > threshold and
        state.num_waiting == 0 and
        (is_nil(state.deliver_subject) or
           not Natsbeam.Sublist.interest?(state.sublist, state.deliver_subject))

    if inactive? do
      Logger.debug(
        "natsbeam: jetstream consumer #{state.stream}/#{state.name} inactive, removing"
      )

      JStream.detach_consumer(state.stream_pid, state.name, :deleted)
      if state.dir, do: File.rm_rf(state.dir)
      {:stop, :normal, %{state | dir: nil, dirty: false}}
    else
      {:noreply, schedule(state, :inactive, 1_000)}
    end
  end

  def handle_info(:unpause, state), do: {:noreply, drain(state)}

  def handle_info(:save, state) do
    state = if state.dirty, do: save_state(state), else: state
    {:noreply, schedule(state, :save, 2_000)}
  end

  def handle_info({:DOWN, _ref, :process, pid, _}, %{stream_pid: pid} = state) do
    state = fail_waiting(state, "409", "Leadership Change")
    {:stop, :normal, state}
  end

  def handle_info(_, state), do: {:noreply, state}

  @impl true
  def terminate(_reason, state) do
    if state.dirty, do: save_state(state)
    :ok
  end

  # --------------------------------------------------------------- delivery

  # Deliver as much as the mode allows: to waiting pull requests, or to the
  # push deliver subject while there is interest and ack-pending headroom.
  defp drain(state) do
    cond do
      paused?(state) -> state
      is_nil(state.deliver_subject) -> drain_pull(state)
      true -> drain_push(state)
    end
  end

  defp drain_pull(state) do
    case :queue.peek(state.waiting) do
      :empty ->
        state

      {:value, req} ->
        cond do
          ack_blocked?(state) ->
            state

          true ->
            case next_message(state) do
              nil ->
                if req.no_wait do
                  {_, state} = take_request(state, req.ref)
                  send_status(state, req.reply, "404", "No Messages", pending_headers(req))
                  drain_pull(state)
                else
                  state
                end

              {msg, state} ->
                size = msg_size(msg)

                if req.max_bytes > 0 and size > req.bytes_left and req.received > 0 do
                  {_, state} = take_request(state, req.ref)

                  send_status(
                    state,
                    req.reply,
                    "409",
                    "Message Size Exceeds MaxBytes",
                    pending_headers(req)
                  )

                  drain_pull(%{state | redeliver: :gb_sets.add(elem(msg, 0), state.redeliver)})
                else
                  state = deliver(state, msg, req.reply)

                  req = %{
                    req
                    | batch_left: req.batch_left - 1,
                      bytes_left: req.bytes_left - size,
                      received: req.received + 1
                  }

                  state =
                    if req.batch_left <= 0 or (req.max_bytes > 0 and req.bytes_left <= 0) do
                      {_, state} = take_request(state, req.ref)
                      state
                    else
                      {{:value, _}, rest} = :queue.out(state.waiting)
                      %{state | waiting: :queue.in_r(req, rest)}
                    end

                  drain_pull(state)
                end
            end
        end
    end
  end

  defp drain_push(state) do
    cond do
      ack_blocked?(state) ->
        state

      not Natsbeam.Sublist.interest?(state.sublist, state.deliver_subject) ->
        schedule(state, :retry, 500)

      true ->
        case next_message(state) do
          nil -> state
          {msg, state} -> drain_push(deliver(state, msg, state.deliver_subject))
        end
    end
  end

  defp ack_blocked?(state) do
    state.ack_policy != "none" and state.max_ack_pending > 0 and
      map_size(state.pending) >= state.max_ack_pending
  end

  # Redeliveries first (lowest stream seq), then the next unseen message.
  defp next_message(state) do
    case :gb_sets.is_empty(state.redeliver) do
      false ->
        {sseq, rest} = :gb_sets.take_smallest(state.redeliver)
        state = %{state | redeliver: rest}

        case Store.get(state.tab, sseq) do
          nil -> next_message(state)
          msg -> {msg, state}
        end

      true ->
        case Store.next_matching(state.tab, state.cursor, state.filters) do
          nil ->
            nil

          {seq, _, _, _, _} = msg ->
            {msg, %{state | cursor: seq, num_pending: max(state.num_pending - 1, 0)}}
        end
    end
  end

  defp deliver(state, {sseq, subject, hdrs, data, ts}, target) do
    {count, state} =
      case Map.get(state.pending, sseq) do
        {_c, n, _} -> {n + 1, %{state | num_redelivered: state.num_redelivered + 1}}
        nil -> {1, state}
      end

    dseq = state.dseq + 1
    now = now_ms()

    reply =
      "$JS.ACK.#{state.stream}.#{state.name}.#{count}.#{sseq}.#{dseq}.#{ts}.#{state.num_pending}"

    {hdrs, data} =
      if state.config["headers_only"] do
        {add_header(hdrs, "Nats-Msg-Size", Integer.to_string(byte_size(data))), ""}
      else
        {hdrs, data}
      end

    Account.publish_routed(state.sublist, target, subject, reply, hdrs, data)

    state = %{
      state
      | dseq: dseq,
        sseq: max(state.sseq, sseq),
        last_sent_ms: now,
        last_active: Util.now_ns(),
        dirty: true
    }

    if state.ack_policy == "none" do
      %{state | ack_floor_c: dseq, ack_floor_s: sseq}
    else
      %{state | pending: Map.put(state.pending, sseq, {dseq, count, now})}
    end
  end

  defp add_header(nil, k, v), do: Headers.encode([{k, v}])

  defp add_header(block, k, v) do
    String.replace_suffix(block, "\r\n", "") <> k <> ": " <> v <> "\r\n\r\n"
  end

  defp msg_size({_, subject, hdrs, data, _}),
    do: byte_size(subject) + byte_size(hdrs || "") + byte_size(data)

  # ------------------------------------------------------------------- acks

  defp parse_ack_subject(subject) do
    case String.split(subject, ".") do
      ["$JS", "ACK", _stream, _consumer, _count, sseq, _cseq, _ts, _pending] -> int_or_error(sseq)
      ["$JS", "ACK", _domain, _hash, _stream, _consumer, _count, sseq | _] -> int_or_error(sseq)
      _ -> :error
    end
  end

  defp int_or_error(s) do
    case Integer.parse(s) do
      {n, ""} -> {:ok, n}
      _ -> :error
    end
  end

  defp handle_ack(state, sseq, payload, reply) do
    state = %{state | last_active: Util.now_ns()}

    state =
      case String.trim(payload) do
        "" -> ack(state, sseq)
        "+ACK" -> ack(state, sseq)
        "-NAK" -> nak(state, sseq, nil)
        "-NAK " <> json -> nak(state, sseq, json)
        "+WPI" -> in_progress(state, sseq)
        "+TERM" <> _ -> settle(state, sseq, :term)
        "+NXT" <> rest -> state |> ack(sseq) |> pull_request(reply, String.trim(rest), false)
        _ -> state
      end

    # Double-ack: the client asked for confirmation.
    if reply && not String.starts_with?(String.trim(payload), "+NXT") do
      Local.publish(reply, "", account: state.account)
    end

    drain(state)
  end

  defp ack(%{ack_policy: "all"} = state, sseq) do
    state.pending
    |> Map.keys()
    |> Enum.filter(&(&1 <= sseq))
    |> Enum.reduce(state, &settle(&2, &1, :ack))
  end

  defp ack(state, sseq), do: settle(state, sseq, :ack)

  defp nak(state, sseq, json) do
    if Map.has_key?(state.pending, sseq) do
      delay_ms =
        with json when is_binary(json) <- json,
             {:ok, %{"delay" => d}} when is_integer(d) <- Jason.decode(json) do
          Util.ns_to_ms(d)
        else
          _ -> nil
        end

      if delay_ms do
        Process.send_after(self(), {:redeliver, sseq}, delay_ms)
        state
      else
        if redeliverable?(state, sseq),
          do: %{state | redeliver: :gb_sets.add(sseq, state.redeliver)},
          else: state
      end
    else
      state
    end
  end

  defp in_progress(state, sseq) do
    case Map.get(state.pending, sseq) do
      nil ->
        state

      {cseq, count, _} ->
        %{state | pending: Map.put(state.pending, sseq, {cseq, count, now_ms()})}
    end
  end

  defp redeliverable?(state, sseq) do
    case Map.get(state.pending, sseq) do
      {_, count, _} -> state.max_deliver <= 0 or count < state.max_deliver
      nil -> true
    end
  end

  # Remove a message from pending (acked, terminated or gone) and move the ack floor.
  defp settle(state, sseq, how) do
    case Map.pop(state.pending, sseq) do
      {nil, _} ->
        state

      {_, pending} ->
        redeliver = :gb_sets.delete_any(sseq, state.redeliver)
        state = %{state | pending: pending, redeliver: redeliver, dirty: true}
        if how in [:ack, :term], do: JStream.acked(state.stream_pid, state.name, sseq)
        update_floor(state)
    end
  end

  defp update_floor(%{pending: pending} = state) when pending == %{} do
    %{state | ack_floor_c: state.dseq, ack_floor_s: state.sseq}
  end

  defp update_floor(state) do
    {min_s, {min_c, _, _}} = Enum.min_by(state.pending, fn {s, _} -> s end)
    %{state | ack_floor_c: min_c - 1, ack_floor_s: min_s - 1}
  end

  defp ack_wait_for(state, count) do
    case state.config["backoff"] do
      [_ | _] = backoff ->
        Util.ns_to_ms(Enum.at(backoff, min(count - 1, length(backoff) - 1))) || state.ack_wait_ms

      _ ->
        state.ack_wait_ms
    end
  end

  # --------------------------------------------------------------- pulling

  defp pull_request(state, reply, body, ack_first? \\ true)
  defp pull_request(state, nil, _body, _), do: state

  defp pull_request(state, reply, body, _ack_first?) do
    opts =
      case Util.decode(body) do
        {:ok, map} when map_size(map) > 0 ->
          map

        _ ->
          case Integer.parse(String.trim(body || "")) do
            {n, ""} -> %{"batch" => n}
            _ -> %{}
          end
      end

    batch = max(opts["batch"] || 1, 1)

    batch =
      if is_integer(state.config["max_batch"]) and state.config["max_batch"] > 0,
        do: min(batch, state.config["max_batch"]),
        else: batch

    expires_ms = Util.ns_to_ms(opts["expires"])
    hb_ms = Util.ns_to_ms(opts["idle_heartbeat"])
    max_bytes = opts["max_bytes"] || 0
    ref = make_ref()

    req = %{
      ref: ref,
      reply: reply,
      batch_left: batch,
      bytes_left: if(max_bytes > 0, do: max_bytes, else: 0),
      max_bytes: max_bytes,
      received: 0,
      no_wait: opts["no_wait"] == true,
      hb_ms: hb_ms
    }

    state = %{state | last_active: Util.now_ns()}
    max_waiting = state.config["max_waiting"] || 512

    cond do
      state.deliver_subject != nil ->
        send_status(state, reply, "409", "Consumer is push based", [])
        state

      state.num_waiting >= max_waiting ->
        send_status(state, reply, "409", "Exceeded MaxWaiting", [])
        state

      true ->
        if expires_ms, do: Process.send_after(self(), {:expire, ref}, expires_ms)
        if hb_ms, do: Process.send_after(self(), {:request_hb, ref}, hb_ms)

        state = %{
          state
          | waiting: :queue.in(req, state.waiting),
            num_waiting: state.num_waiting + 1
        }

        drain(state)
    end
  end

  defp take_request(state, ref) do
    list = :queue.to_list(state.waiting)

    case Enum.split_with(list, &(&1.ref == ref)) do
      {[req], rest} ->
        {req, %{state | waiting: :queue.from_list(rest), num_waiting: length(rest)}}

      {[], _} ->
        {nil, state}
    end
  end

  defp find_request(state, ref), do: Enum.find(:queue.to_list(state.waiting), &(&1.ref == ref))

  defp fail_waiting(state, code, desc) do
    for req <- :queue.to_list(state.waiting),
        do: send_status(state, req.reply, code, desc, pending_headers(req))

    %{state | waiting: :queue.new(), num_waiting: 0}
  end

  defp pending_headers(req) do
    hdrs = [{"Nats-Pending-Messages", req.batch_left}]
    if req.max_bytes > 0, do: hdrs ++ [{"Nats-Pending-Bytes", max(req.bytes_left, 0)}], else: hdrs
  end

  defp send_status(state, target, code, desc, fields) do
    block = Headers.encode(fields, code, desc)
    Account.publish(state.sublist, target, nil, block, "", nil, true)
    :ok
  end

  # ------------------------------------------------------------------- info

  @doc false
  def build_info(state) do
    %{
      "stream_name" => state.stream,
      "name" => state.name,
      "created" => Util.ts(state.created),
      "config" => state.config,
      "delivered" => %{
        "consumer_seq" => state.dseq,
        "stream_seq" => state.sseq,
        "last_active" => Util.ts(state.last_active)
      },
      "ack_floor" => %{"consumer_seq" => state.ack_floor_c, "stream_seq" => state.ack_floor_s},
      "num_ack_pending" => map_size(state.pending),
      "num_redelivered" => state.num_redelivered,
      "num_waiting" => state.num_waiting,
      "num_pending" => state.num_pending,
      "paused" => paused?(state),
      "push_bound" =>
        state.deliver_subject != nil and
          Natsbeam.Sublist.interest?(state.sublist, state.deliver_subject),
      "ts" => Util.ts(Util.now_ns())
    }
    |> then(fn info ->
      if paused?(state),
        do: Map.put(info, "pause_remaining", state.pause_until - Util.now_ns()),
        else: info
    end)
    |> then(fn info ->
      if length(Cluster.nodes()) > 1 do
        Map.put(info, "cluster", %{
          "name" => Cluster.cluster_name(),
          "leader" => Cluster.server_name()
        })
      else
        info
      end
    end)
  end

  # ------------------------------------------------------------ persistence

  defp save_meta(%{dir: nil}), do: :ok

  defp save_meta(state) do
    File.mkdir_p!(state.dir)

    File.write!(
      Path.join(state.dir, "meta.json"),
      Jason.encode!(%{"config" => state.config, "created" => state.created})
    )
  end

  defp checkpoint_data(state) do
    %{
      "dseq" => state.dseq,
      "sseq" => state.sseq,
      "cursor" => state.cursor,
      "ack_floor_c" => state.ack_floor_c,
      "ack_floor_s" => state.ack_floor_s,
      "num_redelivered" => state.num_redelivered,
      "pending" => for({s, {c, n, _}} <- state.pending, do: [s, c, n])
    }
  end

  # Durable consumers checkpoint to disk and to the stream (which replicates it).
  defp save_state(%{durable?: false} = state), do: %{state | dirty: false}

  defp save_state(state) do
    data = checkpoint_data(state)
    if state.dir, do: File.write!(Path.join(state.dir, "state.json"), Jason.encode!(data))

    if Process.alive?(state.stream_pid),
      do: JStream.consumer_checkpoint(state.stream_pid, state.name, data)

    %{state | dirty: false}
  end

  defp restore(state, saved) do
    pending = Map.new(saved["pending"] || [], fn [s, c, n] -> {s, {c, n, 0}} end)

    %{
      state
      | dseq: saved["dseq"] || 0,
        sseq: saved["sseq"] || 0,
        cursor: saved["cursor"] || 0,
        ack_floor_c: saved["ack_floor_c"] || 0,
        ack_floor_s: saved["ack_floor_s"] || 0,
        num_redelivered: saved["num_redelivered"] || 0,
        pending: pending,
        redeliver: :gb_sets.from_list(Map.keys(pending))
    }
  end

  # ---------------------------------------------------------------- timers

  defp schedule(state, key, ms) do
    if t = state.timers[key], do: Process.cancel_timer(t)
    %{state | timers: Map.put(state.timers, key, Process.send_after(self(), key, ms))}
  end

  defp schedule_hb(state) do
    case {state.deliver_subject, Util.ns_to_ms(state.config["idle_heartbeat"])} do
      {nil, _} -> state
      {_, nil} -> state
      {_, ms} -> schedule(state, :hb, ms)
    end
  end

  defp now_ms, do: System.monotonic_time(:millisecond)
end
