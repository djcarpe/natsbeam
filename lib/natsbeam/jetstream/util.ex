defmodule Natsbeam.JetStream.Util do
  @moduledoc false
  # Shared helpers: time formatting, durations, API envelopes and errors.

  @api_prefix "io.nats.jetstream.api.v1."

  # JetStream error codes (from nats-server errors.json) that clients key on.
  @errors %{
    bad_request: {400, 10003, "bad request"},
    not_enabled_for_account: {503, 10039, "JetStream not enabled for account"},
    not_enabled: {503, 10076, "JetStream not enabled"},
    stream_not_found: {404, 10059, "stream not found"},
    stream_name_in_use: {400, 10058, "stream name already in use with a different configuration"},
    stream_subject_overlap: {400, 10065, "subjects overlap with an existing stream"},
    stream_invalid_config: {500, 10052, nil},
    stream_create_err: {500, 10049, nil},
    stream_update_err: {500, 10041, nil},
    stream_sealed: {400, 10109, "invalid operation on sealed stream"},
    stream_purge_not_allowed: {400, 10090, "stream purge not permitted"},
    stream_delete_not_allowed: {400, 10090, "stream delete not permitted"},
    stream_msg_delete_not_allowed: {400, 10057, "message delete not permitted"},
    stream_store_failed: {503, 10077, nil},
    stream_not_match: {400, 10060, "expected stream does not match"},
    stream_wrong_last_sequence: {400, 10071, "wrong last sequence: ~s"},
    stream_wrong_last_msg_id: {400, 10070, "wrong last msg ID: ~s"},
    stream_msg_exceeds_max: {400, 10054, "message size exceeds maximum allowed"},
    stream_max_bytes_required:
      {400, 10113, "account requires a stream config to have max bytes set"},
    stream_msg_delete_failed: {500, 10057, nil},
    stream_replicas_not_supported:
      {500, 10074, "replicas > 1 not supported in non-clustered mode"},
    stream_mirror_not_supported: {400, 10052, "mirror and sourced streams are not supported"},
    no_message_found: {404, 10037, "no message found"},
    sequence_not_found: {400, 10043, "sequence ~s not found"},
    consumer_not_found: {404, 10014, "consumer not found"},
    consumer_name_exist: {400, 10013, "consumer name already in use"},
    consumer_existing_active: {400, 10105, "consumer already exists and is still active"},
    consumer_create_err: {500, 10012, nil},
    consumer_filter_not_subset:
      {400, 10093, "consumer filter subject is not a valid subset of the interest subjects"},
    consumer_pull_requires_ack: {400, 10084, "consumer in pull mode requires ack policy"},
    consumer_durable_name_not_in_subject:
      {400, 10016, "consumer expected to be durable but no durable name set in subject"},
    consumer_durable_name_not_match:
      {400, 10017, "consumer name in subject does not match durable name in request"},
    consumer_deliver_to_wildcards: {400, 10079, "consumer deliver subject has wildcards"},
    consumer_wq_requires_explicit_ack: {400, 10098, "workqueue stream requires explicit ack"},
    consumer_wq_multiple_unfiltered:
      {400, 10099, "multiple non-filtered consumers not allowed on workqueue stream"},
    consumer_wq_not_unique: {400, 10100, "filtered consumer not unique on workqueue stream"},
    consumer_wq_not_deliver_all: {400, 10101, "consumer must be deliver all on workqueue stream"},
    consumer_max_pending_ack_policy_required:
      {400, 10082, "consumer requires ack policy for max ack pending"},
    consumer_invalid_policy: {400, 10094, nil},
    consumer_push_max_waiting: {400, 10080, "consumer in push mode can not set max waiting"},
    consumer_overlapping_filters: {400, 10138, "consumer subject filters cannot overlap"},
    consumer_empty_filter: {400, 10139, "consumer filter in FilterSubjects cannot be empty"},
    consumer_already_exists: {400, 10148, "consumer already exists"},
    consumer_does_not_exist: {400, 10149, "consumer does not exist"},
    memory_resources_exceeded: {500, 10028, "insufficient memory resources available"},
    storage_resources_exceeded: {500, 10047, "insufficient storage resources available"},
    maximum_streams_limit: {400, 10032, "maximum number of streams reached"},
    maximum_consumers_limit: {400, 10026, "maximum consumers limit reached"},
    cluster_no_peers: {400, 10005, "no suitable peers for placement: ~s"},
    cluster_not_leader: {500, 10044, "not the leader for this stream"},
    stream_snapshot_err: {500, 10040, nil},
    stream_restore_err: {500, 10062, nil},
    stream_mirror_not_updatable: {400, 10055, "stream mirror configuration can not be updated"},
    stream_mirror_with_subjects: {400, 10033, "stream mirrors can not contain subjects"},
    stream_mirror_with_sources: {400, 10036, "stream mirrors can not also contain other sources"},
    stream_source_not_found: {400, 10052, "source stream not found: ~s"},
    stream_external_not_supported:
      {400, 10052, "external (cross-account) mirrors and sources are not supported"},
    message_ttl_invalid: {400, 10165, "invalid per-message TTL"},
    message_ttl_disabled: {400, 10166, "per-message TTL is disabled"},
    consumer_paused: {400, 10167, "consumer is paused"},
    not_supported: {400, 10003, "not supported"}
  }

  @doc "Build an API error map `%{code, err_code, description}`."
  def error(key, detail \\ nil)

  def error(key, detail) when is_map_key(@errors, key) do
    {code, err_code, desc} = Map.fetch!(@errors, key)

    description =
      cond do
        is_nil(desc) ->
          to_string(detail || Atom.to_string(key))

        String.contains?(desc, "~s") ->
          :io_lib.format(String.to_charlist(desc), [to_string(detail)]) |> IO.iodata_to_binary()

        true ->
          desc
      end

    %{"code" => code, "err_code" => err_code, "description" => description}
  end

  def error(description, _detail) when is_binary(description),
    do: error(:bad_request, description) |> Map.put("description", description)

  @doc "Wrap a response map (or error) in the typed API envelope."
  def response(type, {:error, %{"code" => _} = err}),
    do: %{"type" => @api_prefix <> type, "error" => err}

  def response(type, {:error, key}) when is_atom(key), do: response(type, {:error, error(key)})
  def response(type, {:error, {key, detail}}), do: response(type, {:error, error(key, detail)})
  def response(type, {:ok, map}), do: response(type, map)
  def response(type, map) when is_map(map), do: Map.put(map, "type", @api_prefix <> type)

  @doc "Current time in nanoseconds since the epoch."
  def now_ns, do: System.os_time(:nanosecond)

  @doc "RFC3339 timestamp (nanosecond precision truncated to microseconds) from ns."
  def ts(ns) when is_integer(ns) do
    DateTime.from_unix!(ns, :nanosecond) |> DateTime.to_iso8601()
  end

  def ts(nil), do: nil

  @doc "Parse an RFC3339 timestamp to ns."
  def parse_ts(nil), do: nil

  def parse_ts(str) when is_binary(str) do
    case DateTime.from_iso8601(str) do
      {:ok, dt, _} -> DateTime.to_unix(dt, :nanosecond)
      _ -> nil
    end
  end

  @doc "Go-duration (ns) to milliseconds, nil for zero/absent."
  def ns_to_ms(nil), do: nil
  def ns_to_ms(0), do: nil
  def ns_to_ms(ns) when is_integer(ns) and ns > 0, do: max(div(ns, 1_000_000), 1)
  def ns_to_ms(_), do: nil

  @doc "Decode a JSON request body; empty body is an empty map."
  def decode(""), do: {:ok, %{}}
  def decode(nil), do: {:ok, %{}}

  def decode(bin) do
    case Jason.decode(bin) do
      {:ok, map} when is_map(map) -> {:ok, map}
      _ -> {:error, :bad_request}
    end
  end

  @doc "Approximate stored size of a message (subject + headers + payload + overhead)."
  def msg_size(subject, hdrs, data),
    do: byte_size(subject) + byte_size(hdrs || "") + byte_size(data) + 16

  @doc "Validate a stream/consumer name (no spaces, dots, wildcards or path separators)."
  def valid_name?(name) when is_binary(name) and name != "" do
    not String.contains?(name, [" ", ".", "*", ">", "/", "\\", "\t", "\r", "\n"])
  end

  def valid_name?(_), do: false

  @doc ~S"""
  Parse a `Nats-TTL` header value: `never`, an integer number of seconds, or
  a Go duration (`1h30m`, `500ms`). Returns `{:ok, :never | ns}` or `:error`.
  """
  def parse_ttl("never"), do: {:ok, :never}

  def parse_ttl(value) when is_binary(value) do
    v = String.trim(value)

    case Integer.parse(v) do
      {secs, ""} when secs > 0 -> {:ok, secs * 1_000_000_000}
      _ -> parse_duration(v)
    end
  end

  def parse_ttl(_), do: :error

  @units %{
    "ns" => 1,
    "us" => 1_000,
    "µs" => 1_000,
    "ms" => 1_000_000,
    "s" => 1_000_000_000,
    "m" => 60_000_000_000,
    "h" => 3_600_000_000_000
  }

  @doc "Parse a Go duration string into nanoseconds."
  def parse_duration(str) do
    case Regex.scan(~r/(\d+(?:\.\d+)?)(ns|us|µs|ms|s|m|h)/, str, capture: :all_but_first) do
      [] ->
        :error

      parts ->
        total =
          Enum.reduce(parts, 0, fn [n, unit], acc ->
            {f, _} = Float.parse(n)
            acc + trunc(f * @units[unit])
          end)

        if total > 0, do: {:ok, total}, else: :error
    end
  end

  @doc "Random name for ephemeral consumers (nats uses 8 chars from a hash alphabet)."
  def random_name do
    :crypto.strong_rand_bytes(6) |> Base.encode32(padding: false) |> String.slice(0, 8)
  end
end
