defmodule Natsbeam.JetStream.API do
  @moduledoc """
  The `$JS.API.>` responder for one account. Receives requests as an in-VM
  subscriber, dispatches to stream/consumer processes and publishes the typed
  JSON responses (or raw messages for `DIRECT.GET`).
  """
  use GenServer, restart: :transient
  require Logger

  alias Natsbeam.{Local, Subject}
  alias Natsbeam.JetStream
  alias Natsbeam.JetStream.{Consumer, Util}
  alias Natsbeam.JetStream.Stream, as: JStream
  alias Natsbeam.Protocol.Headers

  def via(account), do: {:via, Registry, {Natsbeam.Registry, {:js_api, account}}}

  def start_link({account, sublist}),
    do: GenServer.start_link(__MODULE__, {account, sublist}, name: via(account))

  # Every node runs a responder per account; the queue group makes sure a
  # request forwarded across the cluster is answered exactly once.
  @queue "_natsbeam_jsapi"

  @impl true
  def init({account, nil}) do
    Logger.debug("natsbeam: jetstream api started for account #{account}")
    watch_domain()
    {:ok, account |> base_state() |> sync(true)}
  end

  # Started during Account.init: insert into the (public) sublist directly and
  # ask the account to monitor us once it is up.
  def init({account, sublist}) do
    sid = make_ref()
    Natsbeam.Sublist.insert(sublist, self(), sid, "$JS.API.>", @queue)
    Natsbeam.Account.adopt(account, self())
    Logger.debug("natsbeam: jetstream api enabled for account #{account}")
    watch_domain()
    # The account is still in init here; subscribe to the domain subject once it is up.
    send(self(), :config_changed)

    apid =
      case Natsbeam.Account.lookup(account) do
        {:ok, pid, _} -> pid
        :error -> nil
      end

    {:ok, %{base_state(account) | sid: sid} |> watch_account(apid)}
  end

  @doc """
  Bring the responder in line with the current configuration, in place:
  subscribe to `$JS.API.>` (and `$JS.<domain>.API.>`) while the account has
  JetStream, unsubscribe while it has not, and resubscribe if the account
  process was restarted. Synchronous, so once it returns a JetStream-enabled
  account has a responder in its sublist.
  """
  def sync_now(pid), do: GenServer.call(pid, :sync, 15_000)

  defp base_state(account) do
    %{
      account: account,
      apid: nil,
      account_ref: nil,
      sid: nil,
      total: 0,
      errors: 0,
      restores: %{},
      domain: nil,
      domain_sid: nil
    }
  end

  defp watch_account(state, apid) do
    if state.account_ref, do: Process.demonitor(state.account_ref, [:flush])
    ref = if apid, do: Process.monitor(apid)
    %{state | apid: apid, account_ref: ref}
  end

  # Never stop/start the process on config changes: a stop leaves a window
  # with no responder (and a registered-but-dead pid) that racing callers
  # observe as `no_responders`.
  defp sync(state, create? \\ false) do
    if JetStream.enabled_for?(state.account) do
      state |> subscribe_api(create?) |> sync_domain()
    else
      unsubscribe_api(state)
    end
  end

  defp subscribe_api(state, create?) do
    case Natsbeam.Account.lookup(state.account) do
      {:ok, apid, _} when apid == state.apid and state.sid != nil ->
        state

      # The account is gone (removed): do not resurrect it by subscribing.
      :error when not create? ->
        %{state | sid: nil, domain: nil, domain_sid: nil}

      _ ->
        # First subscription, or the account process was restarted (its
        # sublist, and our subscriptions in it, are gone).
        state = %{state | sid: nil, domain: nil, domain_sid: nil}

        case subscribe(state, "$JS.API.>") do
          {:ok, sid} ->
            apid =
              case Natsbeam.Account.lookup(state.account) do
                {:ok, pid, _} -> pid
                :error -> nil
              end

            %{state | sid: sid} |> watch_account(apid)

          {:error, reason} ->
            Logger.warning(
              "natsbeam: jetstream api for #{state.account} could not subscribe: #{inspect(reason)}"
            )

            state
        end
    end
  end

  defp unsubscribe_api(state) do
    for sid <- [state.sid, state.domain_sid], sid != nil, do: unsubscribe(state, sid)
    %{state | sid: nil, domain: nil, domain_sid: nil}
  end

  # The account may be shutting down underneath us (removal): never crash on it.
  defp subscribe(state, subject) do
    Local.subscribe(subject, account: state.account, pid: self(), queue: @queue)
  catch
    :exit, reason -> {:error, reason}
  end

  defp unsubscribe(state, sid) do
    Local.unsubscribe(sid, account: state.account)
  catch
    :exit, _ -> :ok
  end

  # Follow the configured JetStream domain: answer `$JS.<domain>.API.>` too.
  defp watch_domain do
    :telemetry.attach(
      {__MODULE__, self()},
      [:natsbeam, :config, :changed],
      fn _, _, _, pid -> send(pid, :config_changed) end,
      self()
    )
  end

  defp sync_domain(%{sid: nil} = state), do: state

  defp sync_domain(state) do
    domain = JetStream.domain()

    if domain == state.domain do
      state
    else
      if state.domain_sid, do: unsubscribe(state, state.domain_sid)

      case domain && subscribe(state, "$JS.#{domain}.API.>") do
        {:ok, sid} -> %{state | domain: domain, domain_sid: sid}
        nil -> %{state | domain: nil, domain_sid: nil}
        # retried on the next sync
        {:error, _} -> %{state | domain: nil, domain_sid: nil}
      end
    end
  end

  # Restore sessions: chunks arrive on a per-restore inbox; an empty chunk ends it.
  @impl true
  def handle_info(
        {:natsbeam_msg, _sid, "$JS.SNAPSHOT.RESTORE." <> _ = subject, reply, _h, chunk},
        state
      ) do
    case Map.get(state.restores, subject) do
      nil ->
        {:noreply, state}

      %{chunks: chunks, cfg: cfg, sid: sid, timer: timer} = session ->
        if chunk == "" do
          Process.cancel_timer(timer)
          Local.unsubscribe(sid, account: state.account)
          archive = IO.iodata_to_binary(Enum.reverse(chunks))

          archive =
            case Natsbeam.JetStream.Snappy.decode(archive) do
              {:ok, raw} -> raw
              {:error, _} -> archive
            end

          # jsm.go expects the final restore reply typed as a stream create response.
          resp =
            Util.response(
              "stream_create_response",
              JetStream.restore_stream(state.account, cfg, archive)
            )

          if reply, do: Local.publish(reply, Jason.encode!(resp), account: state.account)
          {:noreply, %{state | restores: Map.delete(state.restores, subject)}}
        else
          if reply, do: Local.publish(reply, "", account: state.account)
          Process.cancel_timer(timer)
          timer = Process.send_after(self(), {:restore_timeout, subject}, 60_000)
          session = %{session | chunks: [chunk | chunks], timer: timer}
          {:noreply, %{state | restores: Map.put(state.restores, subject, session)}}
        end
    end
  end

  def handle_info({:restore_timeout, subject}, state) do
    case Map.pop(state.restores, subject) do
      {nil, _} ->
        {:noreply, state}

      {%{sid: sid}, restores} ->
        Local.unsubscribe(sid, account: state.account)
        {:noreply, %{state | restores: restores}}
    end
  end

  def handle_info(
        {:natsbeam_msg, _sid, "$JS.API.CONSUMER.MSG.NEXT." <> rest, reply, _headers, payload},
        state
      ) do
    state = %{state | total: state.total + 1}

    with [stream, name] <- String.split(rest, ".", parts: 2),
         {:ok, pid} <- Consumer.lookup(state.account, stream, name) do
      Consumer.next_request(pid, reply, payload)
      {:noreply, state}
    else
      _ ->
        if reply,
          do:
            Local.publish(reply, "",
              account: state.account,
              headers: Headers.encode([], "404", "Consumer Not Found")
            )

        {:noreply, %{state | errors: state.errors + 1}}
    end
  end

  def handle_info({:natsbeam_msg, sid, "$JS." <> rest, reply, headers, payload}, state)
      when binary_part(rest, 0, 4) != "API." do
    # "$JS.<domain>.API.<op>" → treat as "$JS.API.<op>"; the KV/Object subject
    # spaces are mapped like nats-server does ($JS.<domain>.API.$KV.> → $KV.>) so
    # clients on the far side of a leaf link can write buckets in this domain.
    case String.split(rest, ".", parts: 2) do
      [_domain, "API.$KV." <> kv] ->
        Local.publish("$KV." <> kv, payload,
          account: state.account,
          reply: reply,
          headers: headers
        )

        {:noreply, state}

      [_domain, "API.$OBJ." <> obj] ->
        Local.publish("$OBJ." <> obj, payload,
          account: state.account,
          reply: reply,
          headers: headers
        )

        {:noreply, state}

      [_domain, "API." <> _ = api] ->
        handle_info({:natsbeam_msg, sid, "$JS." <> api, reply, headers, payload}, state)

      _ ->
        {:noreply, state}
    end
  end

  def handle_info({:natsbeam_msg, _sid, "$JS.API." <> op, reply, headers, payload}, state) do
    state = %{state | total: state.total + 1}

    result =
      try do
        handle(op, payload, headers, state)
      rescue
        e ->
          Logger.warning(
            "natsbeam: jetstream api #{op} crashed: #{Exception.message(e)}\n#{Exception.format_stacktrace(__STACKTRACE__)}"
          )

          {:json, Util.response("error_response", {:error, {:bad_request, Exception.message(e)}})}
      catch
        :exit, {reason, _} when reason in [:normal, :noproc, :shutdown, :timeout] ->
          # The stream or consumer went away (deleted, leader moved) while we were asking it.
          {:json,
           Util.response(
             "error_response",
             {:error, {:bad_request, "resource unavailable: #{inspect(reason)}"}}
           )}

        :exit, reason ->
          {:json,
           Util.response(
             "error_response",
             {:error, {:bad_request, "resource unavailable: #{inspect(reason)}"}}
           )}
      end

    state =
      case result do
        {:json, map} when is_map(map) ->
          if reply, do: Local.publish(reply, Jason.encode!(map), account: state.account)
          if Map.has_key?(map, "error"), do: %{state | errors: state.errors + 1}, else: state

        {:raw, hdrs, body} ->
          if reply, do: Local.publish(reply, body, account: state.account, headers: hdrs)
          state
      end

    {:noreply, state}
  end

  def handle_info(:config_changed, state), do: {:noreply, sync(state)}

  # Our account process went away: its sublist (and our subscriptions) died
  # with it. A restarted account is picked up on the next sync.
  def handle_info({:DOWN, ref, :process, _, _}, %{account_ref: ref} = state),
    do: {:noreply, %{state | apid: nil, account_ref: nil, sid: nil, domain: nil, domain_sid: nil}}

  def handle_info({:restore_registered, subject, session}, state),
    do: {:noreply, %{state | restores: Map.put(state.restores, subject, session)}}

  def handle_info(_, state), do: {:noreply, state}

  # Stream the archive to the client in chunks, waiting for each flow-control
  # ack, then an empty message to mark the end.
  defp send_snapshot(account, deliver, archive, chunk_size) do
    inbox = "_INBOX.snap.#{Util.random_name()}"
    {:ok, sid} = Natsbeam.subscribe(inbox, account: account)

    # Clients subscribe to the deliver subject only after our response (and a
    # request-mux wildcard may already match it), so wait for an exact subscription.
    {:ok, _, sublist} = Natsbeam.Account.ensure(account)

    Enum.find(1..50, fn _ ->
      Natsbeam.Sublist.exact_interest?(sublist, deliver) || (Process.sleep(100) && false)
    end)

    # Clients expect a Snappy/S2 framed stream.
    archive = Natsbeam.JetStream.Snappy.encode(archive)
    chunks = for <<chunk::binary-size(chunk_size) <- archive>>, do: chunk

    rest =
      binary_part(
        archive,
        byte_size(archive) - rem(byte_size(archive), chunk_size),
        rem(byte_size(archive), chunk_size)
      )

    chunks = if rest == "", do: chunks, else: chunks ++ [rest]

    Enum.each(chunks, fn chunk ->
      Natsbeam.publish(deliver, chunk, account: account, reply: inbox)

      receive do
        {:natsbeam_msg, ^sid, _, _, _, _} -> :ok
      after
        5_000 -> :ok
      end
    end)

    Natsbeam.publish(deliver, "", account: account)
    Natsbeam.unsubscribe(sid, account: account)
  end

  @impl true
  def handle_call(:sync, _from, state), do: {:reply, :ok, sync(state)}

  def handle_call(:api_stats, _from, state),
    do: {:reply, %{"total" => state.total, "errors" => state.errors}, state}

  # ------------------------------------------------------------- dispatch

  defp handle("INFO", _body, _h, state) do
    Process.put(:api_stats, %{"total" => state.total, "errors" => state.errors})
    {:json, Util.response("account_info_response", JetStream.account_info(state.account))}
  end

  # -- streams

  defp handle("STREAM.CREATE." <> name, body, _h, state),
    do: {:json, Util.response("stream_create_response", create_stream(state.account, name, body))}

  defp handle("STREAM.UPDATE." <> name, body, _h, state),
    do: {:json, Util.response("stream_update_response", update_stream(state.account, name, body))}

  defp handle("STREAM.NAMES", body, _h, state) do
    {:ok, req} = Util.decode(body)
    names = JStream.list(state.account) |> filter_streams(state.account, req["subject"])
    {:json, Util.response("stream_names_response", paged("streams", names, req))}
  end

  defp handle("STREAM.LIST", body, _h, state) do
    {:ok, req} = Util.decode(body)

    infos =
      state.account
      |> JStream.list()
      |> filter_streams(state.account, req["subject"])
      |> Enum.flat_map(fn name ->
        case JStream.lookup(state.account, name) do
          {:ok, pid} -> [JStream.info(pid, %{})]
          _ -> []
        end
      end)

    {:json, Util.response("stream_list_response", paged("streams", infos, req))}
  end

  defp handle("STREAM.INFO." <> name, body, _h, state) do
    {:ok, req} = Util.decode(body)

    resp =
      with {:ok, pid} <- JStream.lookup(state.account, name) do
        info = JStream.info(pid, req)
        # Paginate large subject maps like nats-server (offset/total).
        case info["state"]["subjects"] do
          subjects when is_map(subjects) and map_size(subjects) > 0 ->
            offset = req["offset"] || 0

            keys =
              subjects |> Map.keys() |> Enum.sort() |> Enum.drop(offset) |> Enum.take(100_000)

            info
            |> put_in(["state", "subjects"], Map.take(subjects, keys))
            |> Map.merge(%{"total" => map_size(subjects), "offset" => offset, "limit" => 100_000})

          _ ->
            info
        end
      end

    {:json, Util.response("stream_info_response", resp)}
  end

  defp handle("STREAM.SNAPSHOT." <> name, body, _h, state) do
    {:ok, req} = Util.decode(body)
    account = state.account

    resp =
      with deliver when is_binary(deliver) and deliver != "" <-
             req["deliver_subject"] || {:error, {:bad_request, "deliver subject required"}},
           {:ok, pid} <- JStream.lookup(account, name),
           {:ok, archive, info} <- JStream.snapshot(pid, req["no_consumers"] == true) do
        chunk_size = max(req["chunk_size"] || 128 * 1024, 1024)

        Task.Supervisor.start_child(Natsbeam.JetStream.TaskSupervisor, fn ->
          send_snapshot(account, deliver, archive, chunk_size)
        end)

        %{"config" => info["config"], "state" => info["state"]}
      else
        {:error, _} = e -> e
        _ -> {:error, :bad_request}
      end

    {:json, Util.response("stream_snapshot_response", resp)}
  end

  defp handle("STREAM.RESTORE." <> name, body, _h, state) do
    with {:ok, req} <- Util.decode(body),
         cfg when is_map(cfg) <- req["config"] || {:error, :bad_request},
         true <-
           cfg["name"] == name ||
             {:error, {:bad_request, "stream name in subject does not match request"}},
         {:error, _} <-
           JStream.lookup(state.account, name)
           |> then(&if(match?({:ok, _}, &1), do: :exists, else: &1)) do
      subject = "$JS.SNAPSHOT.RESTORE.#{name}.#{Util.random_name()}"
      {:ok, sid} = Local.subscribe(subject, account: state.account, pid: self())
      timer = Process.send_after(self(), {:restore_timeout, subject}, 60_000)
      session = %{cfg: cfg, chunks: [], sid: sid, timer: timer}
      send(self(), {:restore_registered, subject, session})
      {:json, Util.response("stream_restore_response", %{"deliver_subject" => subject})}
    else
      :exists -> {:json, Util.response("stream_restore_response", {:error, :stream_name_in_use})}
      {:error, _} = e -> {:json, Util.response("stream_restore_response", e)}
      _ -> {:json, Util.response("stream_restore_response", {:error, :bad_request})}
    end
  end

  defp handle("STREAM.LEADER.STEPDOWN." <> name, _body, _h, state) do
    resp =
      with {:ok, pid} <- JStream.lookup(state.account, name),
           :ok <- JStream.stepdown(pid) do
        %{"success" => true}
      end

    {:json, Util.response("stream_leader_stepdown_response", resp)}
  end

  defp handle("STREAM.PEER.REMOVE." <> name, body, _h, state) do
    {:ok, req} = Util.decode(body)

    resp =
      with peer when is_binary(peer) <-
             req["peer"] || {:error, {:bad_request, "peer name required"}},
           {:ok, pid} <- JStream.lookup(state.account, name),
           :ok <- JStream.remove_peer(pid, peer) do
        %{"success" => true}
      end

    {:json, Util.response("stream_remove_peer_response", resp)}
  end

  defp handle("META.LEADER.STEPDOWN", _body, _h, _state),
    do: {:json, Util.response("meta_leader_stepdown_response", %{"success" => true})}

  defp handle("META.SERVER.REMOVE", _body, _h, _state),
    do:
      {:json,
       Util.response(
         "meta_server_remove_response",
         {:error, {:not_supported, "server removal is not supported; stop the node instead"}}
       )}

  defp handle("STREAM.DELETE." <> name, _body, _h, state) do
    resp =
      with {:ok, pid} <- JStream.lookup(state.account, name),
           :ok <- JStream.delete(pid) do
        %{"success" => true}
      end

    {:json, Util.response("stream_delete_response", resp)}
  end

  defp handle("STREAM.PURGE." <> name, body, _h, state) do
    {:ok, req} = Util.decode(body)

    resp =
      with {:ok, pid} <- JStream.lookup(state.account, name),
           {:ok, n} <- JStream.purge(pid, req) do
        %{"success" => true, "purged" => n}
      end

    {:json, Util.response("stream_purge_response", resp)}
  end

  defp handle("STREAM.MSG.GET." <> name, body, _h, state) do
    {:ok, req} = Util.decode(body)

    resp =
      with {:ok, pid} <- JStream.lookup(state.account, name),
           {:ok, {seq, subject, hdrs, data, ts}} <- JStream.get_msg(pid, req) do
        msg = %{
          "subject" => subject,
          "seq" => seq,
          "data" => Base.encode64(data),
          "time" => Util.ts(ts)
        }

        msg = if hdrs, do: Map.put(msg, "hdrs", Base.encode64(hdrs)), else: msg
        %{"message" => msg}
      end

    {:json, Util.response("stream_msg_get_response", resp)}
  end

  defp handle("STREAM.MSG.DELETE." <> name, body, _h, state) do
    {:ok, req} = Util.decode(body)

    resp =
      with {:ok, pid} <- JStream.lookup(state.account, name),
           true <- is_integer(req["seq"]) || {:error, :bad_request},
           :ok <- JStream.delete_msg(pid, req["seq"], req["no_erase"] == true) do
        %{"success" => true}
      end

    {:json, Util.response("stream_msg_delete_response", resp)}
  end

  defp handle("DIRECT.GET." <> rest, body, _h, state) do
    {name, req} =
      case String.split(rest, ".", parts: 2) do
        [name] ->
          case Util.decode(body) do
            {:ok, req} -> {name, req}
            _ -> {name, :bad}
          end

        [name, subject] ->
          {name, %{"last_by_subj" => subject}}
      end

    case JStream.lookup(state.account, name) do
      {:error, _} ->
        {:raw, Headers.encode([], "404", "Message Not Found"), ""}

      {:ok, pid} ->
        cond do
          req == :bad or req == %{} ->
            {:raw, Headers.encode([], "408", "Empty Request"), ""}

          not JStream.config(pid)["allow_direct"] ->
            {:raw, Headers.encode([], "400", "Direct Get Not Permitted"), ""}

          true ->
            case JStream.get_msg(pid, req) do
              {:ok, {seq, subject, hdrs, data, ts}} ->
                extra = [
                  {"Nats-Stream", name},
                  {"Nats-Subject", subject},
                  {"Nats-Sequence", Integer.to_string(seq)},
                  {"Nats-Time-Stamp", Util.ts(ts)}
                ]

                fields =
                  case hdrs && Headers.decode(hdrs) do
                    {:ok, %{fields: f}} -> f
                    _ -> []
                  end

                {:raw, Headers.encode(fields ++ extra), data}

              {:error, :no_message_found} ->
                {:raw, Headers.encode([], "404", "Message Not Found"), ""}

              {:error, _} ->
                {:raw, Headers.encode([], "408", "Bad Request"), ""}
            end
        end
    end
  end

  # -- consumers

  defp handle("CONSUMER.CREATE." <> rest, body, _h, state) do
    {stream, name, filter} =
      case String.split(rest, ".", parts: 3) do
        [s] -> {s, nil, nil}
        [s, n] -> {s, n, nil}
        [s, n, f] -> {s, n, f}
      end

    {:json,
     Util.response(
       "consumer_create_response",
       create_consumer(state.account, stream, name, filter, body, false)
     )}
  end

  defp handle("CONSUMER.DURABLE.CREATE." <> rest, body, _h, state) do
    case String.split(rest, ".", parts: 2) do
      [stream, name] ->
        {:json,
         Util.response(
           "consumer_create_response",
           create_consumer(state.account, stream, name, nil, body, true)
         )}

      _ ->
        {:json, Util.response("consumer_create_response", {:error, :bad_request})}
    end
  end

  defp handle("CONSUMER.NAMES." <> stream, body, _h, state) do
    {:ok, req} = Util.decode(body)

    resp =
      with {:ok, _} <- JStream.lookup(state.account, stream) do
        paged("consumers", Consumer.list(state.account, stream), req)
      end

    {:json, Util.response("consumer_names_response", resp)}
  end

  defp handle("CONSUMER.LIST." <> stream, body, _h, state) do
    {:ok, req} = Util.decode(body)

    resp =
      with {:ok, _} <- JStream.lookup(state.account, stream) do
        infos =
          for name <- Consumer.list(state.account, stream),
              {:ok, pid} <- [Consumer.lookup(state.account, stream, name)],
              do: Consumer.info(pid)

        paged("consumers", infos, req)
      end

    {:json, Util.response("consumer_list_response", resp)}
  end

  defp handle("CONSUMER.INFO." <> rest, _body, _h, state) do
    resp =
      with [stream, name] <- String.split(rest, ".", parts: 2),
           {:ok, _} <- JStream.lookup(state.account, stream),
           {:ok, pid} <- Consumer.lookup(state.account, stream, name) do
        Consumer.info(pid)
      else
        {:error, _} = e -> e
        _ -> {:error, :bad_request}
      end

    {:json, Util.response("consumer_info_response", resp)}
  end

  defp handle("CONSUMER.DELETE." <> rest, _body, _h, state) do
    resp =
      with [stream, name] <- String.split(rest, ".", parts: 2),
           {:ok, _} <- JStream.lookup(state.account, stream),
           {:ok, pid} <- Consumer.lookup(state.account, stream, name),
           :ok <- Consumer.delete(pid) do
        %{"success" => true}
      else
        {:error, _} = e -> e
        _ -> {:error, :bad_request}
      end

    {:json, Util.response("consumer_delete_response", resp)}
  end

  defp handle("CONSUMER.PAUSE." <> rest, body, _h, state) do
    {:ok, req} = Util.decode(body)

    resp =
      with [stream, name] <- String.split(rest, ".", parts: 2),
           {:ok, _} <- JStream.lookup(state.account, stream),
           {:ok, pid} <- Consumer.lookup(state.account, stream, name) do
        Consumer.pause(pid, req["pause_until"] || "")
      else
        {:error, _} = e -> e
        _ -> {:error, :bad_request}
      end

    {:json, Util.response("consumer_pause_response", resp)}
  end

  defp handle("CONSUMER.LEADER.STEPDOWN." <> rest, _body, _h, state) do
    # Consumers live with their stream leader; moving one means moving the stream.
    resp =
      with [stream, name] <- String.split(rest, ".", parts: 2),
           {:ok, spid} <- JStream.lookup(state.account, stream),
           {:ok, _} <- Consumer.lookup(state.account, stream, name),
           :ok <- JStream.stepdown(spid) do
        %{"success" => true}
      else
        {:error, _} = e -> e
        _ -> {:error, :bad_request}
      end

    {:json, Util.response("consumer_leader_stepdown_response", resp)}
  end

  defp handle(op, _body, _h, _state) do
    {:json,
     Util.response(
       "error_response",
       {:error, {:bad_request, "unsupported JetStream API request: #{op}"}}
     )}
  end

  # ----------------------------------------------------------- streams impl

  defp create_stream(account, name, body) do
    with {:ok, cfg} <- Util.decode(body),
         true <-
           cfg["name"] == name ||
             {:error, {:stream_invalid_config, "stream name in subject does not match request"}},
         {:ok, cfg} <- JStream.normalize_config(cfg),
         :ok <- check_overlap(account, cfg),
         :ok <- check_sources(account, cfg),
         :ok <- JetStream.check_stream_limits(account, cfg) do
      case JStream.lookup(account, name) do
        {:ok, pid} ->
          existing = JStream.config(pid)

          if comparable(existing) == comparable(cfg) do
            {:ok, JStream.info(pid, %{})}
          else
            {:error, :stream_name_in_use}
          end

        {:error, _} ->
          case JetStream.start_stream(account, cfg) do
            {:ok, pid} -> {:ok, JStream.info(pid, %{})}
            {:error, {:already_started, pid}} -> {:ok, JStream.info(pid, %{})}
            {:error, reason} -> {:error, {:stream_create_err, inspect(reason)}}
          end
      end
    end
  end

  defp comparable(cfg), do: Map.drop(cfg, ["metadata"])

  defp check_sources(account, cfg) do
    origins =
      for s <- [cfg["mirror"] | cfg["sources"] || []], s, is_nil(s["external"]), do: s["name"]

    case Enum.find(origins, &match?({:error, _}, JStream.lookup(account, &1))) do
      nil -> :ok
      missing -> {:error, {:stream_source_not_found, missing}}
    end
  end

  defp update_stream(account, name, body) do
    with {:ok, pid} <- JStream.lookup(account, name),
         {:ok, cfg} <- Util.decode(body),
         true <-
           cfg["name"] == name ||
             {:error, {:stream_invalid_config, "stream name in subject does not match request"}},
         {:ok, cfg} <- JStream.normalize_config(cfg),
         :ok <- check_overlap(account, cfg) do
      JStream.update(pid, cfg)
    end
  end

  defp check_overlap(account, cfg) do
    others =
      for name <- JStream.list(account),
          name != cfg["name"],
          {:ok, pid} <- [JStream.lookup(account, name)],
          do: JStream.config(pid)["subjects"] || []

    overlap? =
      Enum.any?(others, fn subjects ->
        Enum.any?(subjects, fn s ->
          Enum.any?(cfg["subjects"] || [], &Subject.intersects?(&1, s))
        end)
      end)

    if overlap?, do: {:error, :stream_subject_overlap}, else: :ok
  end

  defp filter_streams(names, _account, nil), do: names
  defp filter_streams(names, _account, ""), do: names

  defp filter_streams(names, account, subject) do
    Enum.filter(names, fn name ->
      case JStream.lookup(account, name) do
        {:ok, pid} ->
          Enum.any?(JStream.config(pid)["subjects"] || [], &Subject.intersects?(&1, subject))

        _ ->
          false
      end
    end)
  end

  defp paged(key, items, req) do
    offset = req["offset"] || 0
    limit = 256
    page = items |> Enum.drop(offset) |> Enum.take(limit)
    %{"total" => length(items), "offset" => offset, "limit" => limit, key => page}
  end

  # --------------------------------------------------------- consumers impl

  defp create_consumer(account, stream, name_in_subject, filter_in_subject, body, legacy_durable?) do
    with {:ok, spid} <- JStream.lookup(account, stream),
         {:ok, req} <- Util.decode(body),
         cfg = req["config"] || %{},
         true <-
           (req["stream_name"] || stream) == stream ||
             {:error, {:bad_request, "stream name in subject does not match request"}},
         :ok <- check_consumer_name(cfg, name_in_subject, legacy_durable?),
         :ok <- check_consumer_filter(cfg, filter_in_subject) do
      cfg =
        if name_in_subject && is_nil(cfg["name"]) && is_nil(cfg["durable_name"]),
          do: Map.put(cfg, "name", name_in_subject),
          else: cfg

      cfg =
        if legacy_durable? and is_nil(cfg["durable_name"]),
          do: Map.put(cfg, "durable_name", name_in_subject),
          else: cfg

      cfg =
        if filter_in_subject && filter_in_subject != ">" && is_nil(cfg["filter_subject"]) &&
             is_nil(cfg["filter_subjects"]),
           do: Map.put(cfg, "filter_subject", filter_in_subject),
           else: cfg

      stream_cfg = JStream.config(spid)

      existing =
        for n <- Consumer.list(account, stream),
            {:ok, pid} <- [Consumer.lookup(account, stream, n)] do
          c = Consumer.config(pid)
          {n, List.wrap(c["filter_subject"] || c["filter_subjects"] || [])}
        end

      with {:ok, cfg} <- Consumer.normalize_config(stream_cfg, cfg, existing) do
        case {Consumer.lookup(account, stream, cfg["name"]), req["action"]} do
          {{:ok, _pid}, "create"} ->
            {:error, :consumer_already_exists}

          {{:ok, pid}, _} ->
            current = Consumer.config(pid)

            if comparable(current) == comparable(cfg) do
              {:ok, Consumer.info(pid)}
            else
              Consumer.update(pid, cfg)
            end

          {{:error, _}, "update"} ->
            {:error, :consumer_does_not_exist}

          {{:error, _}, _} ->
            with :ok <- JetStream.check_consumer_limits(account),
                 {:ok, pid} <- JetStream.start_consumer(account, stream, cfg) do
              {:ok, Consumer.info(pid)}
            else
              {:error, {:already_started, pid}} -> {:ok, Consumer.info(pid)}
              {:error, %{"code" => _} = err} -> {:error, err}
              {:error, reason} when is_atom(reason) -> {:error, reason}
              {:error, {reason, detail}} when is_atom(reason) -> {:error, {reason, detail}}
              {:error, reason} -> {:error, {:consumer_create_err, inspect(reason)}}
            end
        end
      end
    end
  end

  defp check_consumer_name(cfg, nil, true),
    do: if(cfg["durable_name"], do: :ok, else: {:error, :consumer_durable_name_not_in_subject})

  defp check_consumer_name(_cfg, nil, false), do: :ok

  defp check_consumer_name(cfg, name, legacy?) do
    cond do
      (legacy? and cfg["durable_name"]) && cfg["durable_name"] != name ->
        {:error, :consumer_durable_name_not_match}

      cfg["durable_name"] && cfg["durable_name"] != name ->
        {:error, :consumer_durable_name_not_match}

      cfg["name"] && cfg["name"] != name ->
        {:error, {:bad_request, "consumer name in subject does not match request"}}

      true ->
        :ok
    end
  end

  defp check_consumer_filter(_cfg, nil), do: :ok
  defp check_consumer_filter(_cfg, ">"), do: :ok

  defp check_consumer_filter(cfg, filter) do
    case cfg["filter_subject"] do
      nil -> :ok
      ^filter -> :ok
      _ -> {:error, {:bad_request, "consumer filter in subject does not match request"}}
    end
  end
end
