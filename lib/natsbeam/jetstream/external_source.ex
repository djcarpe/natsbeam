defmodule Natsbeam.JetStream.ExternalSource do
  @moduledoc """
  Pulls messages for a mirror/source whose origin lives behind a JetStream API
  prefix (another domain over a leaf/route link, or another account). It
  creates an ephemeral push consumer on the origin through that API and feeds
  the owning stream `{:ext_msg, origin, seq, subject, headers, payload, ts}`.

  A push consumer only conveys messages, never removals (nats-server's own
  mirrors only skip the gaps they see in delivered sequences). So for mirrors
  the worker also polls the origin's `STREAM.INFO` through the same prefix and
  feeds the owner `{:ext_state, origin, first_seq, deleted}`: everything below
  `first_seq` is gone on the origin (purged, expired, limits) and `deleted`
  (or `nil` when unchanged since the last report) lists the interior sequences
  the origin reports deleted. Both are positive reports of removal, so a
  message that simply has not arrived yet is never touched.
  """
  use GenServer
  require Logger

  alias Natsbeam.Local
  alias Natsbeam.JetStream.Util
  alias Natsbeam.Protocol.Headers

  @heartbeat_ns 5_000_000_000
  @state_interval_ms 2_000

  def start_link(args), do: GenServer.start_link(__MODULE__, args)

  @impl true
  def init(%{owner: owner} = args) do
    Process.monitor(owner)
    inbox = "_INBOX.src.#{Util.random_name()}"
    {:ok, sid} = Local.subscribe(inbox, account: args.account, pid: self())

    state =
      Map.merge(args, %{
        inbox: inbox,
        sid: sid,
        consumer: nil,
        last_seen: System.monotonic_time(:millisecond),
        mirror?: Map.get(args, :mirror?, false),
        state_interval: Map.get(args, :state_interval, @state_interval_ms),
        # {first_seq, last_seq, messages, num_deleted} last reported to the owner
        origin_state: nil,
        polling: nil,
        poll_timer: nil
      })

    if state.mirror?, do: send(self(), :poll_state)
    {:ok, state, {:continue, :create}}
  end

  @impl true
  def handle_continue(:create, state), do: {:noreply, create_consumer(state)}

  defp create_consumer(state) do
    name = "src_" <> Util.random_name()

    cfg =
      %{
        "name" => name,
        "deliver_subject" => state.inbox,
        "deliver_policy" => if(state.last > 0, do: "by_start_sequence", else: "all"),
        "opt_start_seq" => if(state.last > 0, do: state.last + 1),
        "ack_policy" => "none",
        "replay_policy" => "instant",
        "filter_subject" => state.filter,
        "idle_heartbeat" => @heartbeat_ns,
        "flow_control" => true,
        "inactive_threshold" => 60_000_000_000,
        "num_replicas" => 1
      }
      |> Enum.reject(fn {_, v} -> is_nil(v) end)
      |> Map.new()

    body = Jason.encode!(%{"stream_name" => state.origin, "config" => cfg})

    case Local.request("#{state.api}.CONSUMER.CREATE.#{state.origin}.#{name}", body,
           account: state.account,
           timeout: 5_000
         ) do
      {:ok, %{payload: p}} ->
        case Jason.decode(p) do
          {:ok, %{"error" => err}} ->
            Logger.warning(
              "natsbeam: external source #{state.origin} via #{state.api}: #{err["description"]}"
            )

            Process.send_after(self(), :recreate, 2_000)
            state

          {:ok, %{"name" => cname}} ->
            %{state | consumer: cname, last_seen: System.monotonic_time(:millisecond)}
            |> schedule_check()

          _ ->
            Process.send_after(self(), :recreate, 2_000)
            state
        end

      _ ->
        Process.send_after(self(), :recreate, 2_000)
        state
    end
  end

  defp schedule_check(state) do
    Process.send_after(self(), :check, 15_000)
    state
  end

  @impl true
  def handle_info({:natsbeam_msg, _sid, subject, reply, hdrs, payload}, state) do
    state = %{state | last_seen: System.monotonic_time(:millisecond)}

    cond do
      is_binary(reply) and String.starts_with?(reply, "$JS.ACK.") ->
        case ack_meta(reply) do
          {seq, ts} when seq > state.last ->
            # A gap in delivered sequences means the origin removed messages:
            # look at its state now rather than at the next poll.
            if state.mirror? and state.last > 0 and seq > state.last + 1,
              do: send(self(), :poll_state)

            send(state.owner, {:ext_msg, state.origin, seq, subject, hdrs, payload, ts})
            {:noreply, %{state | last: seq}}

          _ ->
            {:noreply, state}
        end

      is_binary(hdrs) ->
        # heartbeats and flow control requests
        case Headers.decode(hdrs) do
          {:ok, h} ->
            cond do
              stalled = Headers.get(h, "Nats-Consumer-Stalled") ->
                Local.publish(stalled, "", account: state.account)

              h.status == "100" and reply ->
                Local.publish(reply, "", account: state.account)

              true ->
                :ok
            end

          _ ->
            :ok
        end

        {:noreply, state}

      true ->
        {:noreply, state}
    end
  end

  def handle_info(:recreate, state), do: {:noreply, create_consumer(state)}

  # -- origin state (mirrors only): follow deletes and purges

  def handle_info(:poll_state, %{polling: nil} = state) do
    if state.poll_timer, do: Process.cancel_timer(state.poll_timer)
    me = self()
    %{api: api, origin: origin, account: account, origin_state: known} = state

    {pid, ref} =
      spawn_monitor(fn -> send(me, {:origin_state, poll(api, origin, account, known)}) end)

    {:noreply, %{state | polling: {pid, ref}, poll_timer: nil}}
  end

  def handle_info(:poll_state, state), do: {:noreply, state}

  def handle_info({:origin_state, {:ok, key, first, deleted}}, state) do
    send(state.owner, {:ext_state, state.origin, first, deleted})
    {:noreply, %{state | origin_state: key}}
  end

  def handle_info({:origin_state, _}, state), do: {:noreply, state}

  def handle_info({:DOWN, ref, :process, _, _}, %{polling: {_, ref}} = state) do
    timer = Process.send_after(self(), :poll_state, state.state_interval)
    {:noreply, %{state | polling: nil, poll_timer: timer}}
  end

  def handle_info(:check, state) do
    idle = System.monotonic_time(:millisecond) - state.last_seen

    if idle > 3 * div(@heartbeat_ns, 1_000_000) do
      Logger.info("natsbeam: external source #{state.origin}: no heartbeats, recreating consumer")
      {:noreply, create_consumer(state)}
    else
      {:noreply, schedule_check(state)}
    end
  end

  def handle_info({:DOWN, _, :process, pid, _}, %{owner: pid} = state),
    do: {:stop, :normal, state}

  def handle_info(_, state), do: {:noreply, state}

  # One STREAM.INFO round, run in a helper process so a slow or unreachable
  # origin never stalls delivery and flow control. The (possibly long) deleted
  # list is only fetched when the origin's shape changed since the last report.
  defp poll(api, origin, account, known) do
    with {:ok, st} <- origin_info(api, origin, account, %{}) do
      key = state_key(st)
      first = st["first_seq"] || 0

      cond do
        key == known ->
          {:ok, key, first, nil}

        (st["num_deleted"] || 0) == 0 ->
          {:ok, key, first, []}

        true ->
          case origin_info(api, origin, account, %{"deleted_details" => true}) do
            {:ok, st} -> {:ok, state_key(st), st["first_seq"] || first, st["deleted"] || []}
            # report the purge horizon now, the interior deletes next round
            _ -> {:ok, known, first, nil}
          end
      end
    end
  end

  defp state_key(st),
    do: {st["first_seq"] || 0, st["last_seq"] || 0, st["messages"] || 0, st["num_deleted"] || 0}

  defp origin_info(api, origin, account, req) do
    with {:ok, %{payload: p}} <-
           Local.request("#{api}.STREAM.INFO.#{origin}", Jason.encode!(req),
             account: account,
             timeout: 2_000
           ),
         {:ok, %{"state" => st}} when is_map(st) <- Jason.decode(p) do
      {:ok, st}
    else
      other -> {:error, other}
    end
  end

  @impl true
  def terminate(_reason, state) do
    if state.consumer do
      Local.publish("#{state.api}.CONSUMER.DELETE.#{state.origin}.#{state.consumer}", "",
        account: state.account
      )
    end

    Local.unsubscribe(state.sid, account: state.account)
    :ok
  end

  # $JS.ACK.<stream>.<consumer>.<delivered>.<sseq>.<cseq>.<ts>.<pending> (v1) or the
  # 12-token v2 form with domain and account hash.
  defp ack_meta(reply) do
    case String.split(reply, ".") do
      ["$JS", "ACK", _s, _c, _d, sseq, _cseq, ts, _pending] ->
        {String.to_integer(sseq), String.to_integer(ts)}

      ["$JS", "ACK", _dom, _hash, _s, _c, _d, sseq, _cseq, ts | _] ->
        {String.to_integer(sseq), String.to_integer(ts)}

      _ ->
        :error
    end
  rescue
    _ -> :error
  end
end
