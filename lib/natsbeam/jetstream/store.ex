defmodule Natsbeam.JetStream.Store do
  @moduledoc """
  Message store for one stream.

  Messages live in an ETS `ordered_set` keyed by stream sequence so consumers
  can walk them without touching the stream process. Two side tables index
  subjects: `by_subject` (`{subject, count, last_seq}`) and `idx`
  (`{{subject, seq}}`) for per-subject first/last/next lookups.

  With `storage: "file"` every mutation is also appended to a length-prefixed,
  CRC-checked log (`msgs.log`) that is replayed on start and compacted when it
  fills up with deletions. The working set stays in memory.
  """
  alias Natsbeam.JetStream.Util
  alias Natsbeam.Subject

  defstruct tab: nil,
            by_subject: nil,
            idx: nil,
            first_seq: 1,
            last_seq: 0,
            last_ts: nil,
            msgs: 0,
            bytes: 0,
            file: nil,
            path: nil,
            log_entries: 0,
            dirty: false

  @type t :: %__MODULE__{}
  @type msg ::
          {seq :: pos_integer(), subject :: binary(), hdrs :: binary() | nil, data :: binary(),
           ts :: integer()}

  @magic "NBJS1"

  # ------------------------------------------------------------------ open

  @doc "Open a store. `dir` nil means memory only."
  @spec open(binary() | nil) :: t()
  def open(dir) do
    opts = [:public, read_concurrency: true]

    store = %__MODULE__{
      tab: :ets.new(:js_msgs, [:ordered_set | opts]),
      by_subject: :ets.new(:js_by_subject, [:set | opts]),
      idx: :ets.new(:js_idx, [:ordered_set | opts])
    }

    if dir do
      File.mkdir_p!(dir)
      path = Path.join(dir, "msgs.log")
      store = replay(%{store | path: path}, path)
      {:ok, file} = :file.open(path, [:append, :binary, :raw, {:delayed_write, 65_536, 200}])
      %{store | file: file}
    else
      store
    end
  end

  def close(%__MODULE__{} = s) do
    if s.file do
      :file.sync(s.file)
      :file.close(s.file)
    end

    for t <- [s.tab, s.by_subject, s.idx], do: :ets.delete(t)
    :ok
  end

  @doc "Remove on-disk data."
  def destroy(%__MODULE__{} = s) do
    close(s)
    if s.path, do: File.rm_rf(Path.dirname(s.path))
    :ok
  end

  def sync(%__MODULE__{file: nil} = s), do: s

  def sync(%__MODULE__{dirty: true} = s) do
    :file.sync(s.file)
    %{s | dirty: false}
  end

  def sync(s), do: s

  # ------------------------------------------------------------------ write

  @doc "Append a message; returns `{seq, store}`."
  @spec append(t(), binary(), binary() | nil, binary(), integer() | nil) :: {pos_integer(), t()}
  def append(%__MODULE__{} = s, subject, hdrs, data, ts \\ nil) do
    seq = s.last_seq + 1
    ts = ts || Util.now_ns()
    s = insert(s, {seq, subject, hdrs, data, ts})
    s = log(s, {:m, seq, subject, hdrs, data, ts})
    {seq, %{s | last_seq: seq, last_ts: ts}}
  end

  defp insert(s, {seq, subject, hdrs, data, _ts} = msg) do
    :ets.insert(s.tab, msg)
    :ets.insert(s.idx, {{subject, seq}})

    case :ets.lookup(s.by_subject, subject) do
      [] -> :ets.insert(s.by_subject, {subject, 1, seq})
      [{_, n, _}] -> :ets.insert(s.by_subject, {subject, n + 1, seq})
    end

    first = if s.msgs == 0, do: seq, else: s.first_seq
    %{s | msgs: s.msgs + 1, bytes: s.bytes + Util.msg_size(subject, hdrs, data), first_seq: first}
  end

  @doc "Append a message with an explicit sequence (mirrors, replicas, restores). `seq` must exceed `last_seq`."
  @spec append_at(t(), pos_integer(), binary(), binary() | nil, binary(), integer()) ::
          {:ok, t()} | {:error, :sequence_in_past}
  def append_at(%__MODULE__{} = s, seq, subject, hdrs, data, ts) do
    if seq <= s.last_seq do
      {:error, :sequence_in_past}
    else
      s = insert(s, {seq, subject, hdrs, data, ts})
      s = log(s, {:m, seq, subject, hdrs, data, ts})
      {:ok, %{s | last_seq: seq, last_ts: ts}}
    end
  end

  @doc "Advance `last_seq` without storing (sequence skipped by the origin/leader)."
  def bump_last_seq(%__MODULE__{} = s, seq) when seq > s.last_seq do
    s = log(s, {:l, seq})
    %{s | last_seq: seq, first_seq: if(s.msgs == 0, do: seq + 1, else: s.first_seq)}
  end

  def bump_last_seq(s, _), do: s

  @doc "Replace the whole contents with `msgs` (sorted by seq) and `last_seq`."
  def load_snapshot(%__MODULE__{} = s, msgs, last_seq) do
    for t <- [s.tab, s.by_subject, s.idx], do: :ets.delete_all_objects(t)
    s = %{s | msgs: 0, bytes: 0, first_seq: 1, last_seq: 0, last_ts: nil}
    s = log(s, {:p, :all})

    s =
      Enum.reduce(msgs, s, fn {seq, subject, hdrs, data, ts}, acc ->
        acc = insert(acc, {seq, subject, hdrs, data, ts})
        acc = log(acc, {:m, seq, subject, hdrs, data, ts})
        %{acc | last_seq: seq, last_ts: ts}
      end)

    if last_seq > s.last_seq, do: bump_last_seq(s, last_seq), else: s
  end

  @doc "Every stored message in sequence order."
  def all(%__MODULE__{tab: tab}), do: :ets.tab2list(tab)

  @doc "Up to `limit` messages with seq > `after_seq` matching `filter` (nil = all)."
  def read_after(%__MODULE__{tab: tab}, after_seq, filter, limit) do
    filters = if filter && filter != ">", do: [filter], else: nil
    do_read(tab, after_seq, filters, limit, [])
  end

  defp do_read(_tab, _seq, _filters, 0, acc), do: Enum.reverse(acc)

  defp do_read(tab, seq, filters, limit, acc) do
    case next_matching(tab, seq, filters) do
      nil -> Enum.reverse(acc)
      {s, _, _, _, _} = msg -> do_read(tab, s, filters, dec(limit), [msg | acc])
    end
  end

  defp dec(:infinity), do: :infinity
  defp dec(n), do: n - 1

  @doc "Delete one message by sequence. Returns `{true | false, store}`."
  @spec delete(t(), pos_integer()) :: {boolean(), t()}
  def delete(%__MODULE__{} = s, seq) do
    case :ets.lookup(s.tab, seq) do
      [] ->
        {false, s}

      [msg] ->
        s = remove(s, msg)
        {true, log(s, {:d, seq})}
    end
  end

  defp remove(s, {seq, subject, hdrs, data, _ts}) do
    :ets.delete(s.tab, seq)
    :ets.delete(s.idx, {subject, seq})

    case :ets.lookup(s.by_subject, subject) do
      [{_, 1, _}] ->
        :ets.delete(s.by_subject, subject)

      [{_, n, last}] ->
        last = if last == seq, do: last_seq_for(s, subject, seq), else: last
        :ets.insert(s.by_subject, {subject, n - 1, last})

      [] ->
        :ok
    end

    msgs = s.msgs - 1

    first =
      if msgs == 0,
        do: s.last_seq + 1,
        else: if(seq == s.first_seq, do: :ets.first(s.tab), else: s.first_seq)

    %{s | msgs: msgs, bytes: s.bytes - Util.msg_size(subject, hdrs, data), first_seq: first}
  end

  defp last_seq_for(s, subject, below) do
    case :ets.prev(s.idx, {subject, below}) do
      {^subject, seq} -> seq
      _ -> 0
    end
  end

  @doc "Remove every message (optionally only those matching `filter`, up to `seq`-1, keeping the last `keep`)."
  @spec purge(t(), keyword()) :: {non_neg_integer(), t()}
  def purge(%__MODULE__{} = s, opts \\ []) do
    filter = Keyword.get(opts, :filter)
    upto = Keyword.get(opts, :seq)
    keep = Keyword.get(opts, :keep)

    cond do
      is_nil(filter) and is_nil(upto) and is_nil(keep) ->
        n = s.msgs
        for t <- [s.tab, s.by_subject, s.idx], do: :ets.delete_all_objects(t)
        s = %{s | msgs: 0, bytes: 0, first_seq: s.last_seq + 1}
        {n, log(s, {:p, :all})}

      true ->
        victims =
          s
          |> stream_msgs(filter)
          |> Enum.filter(fn {seq, _, _, _, _} -> is_nil(upto) or seq < upto end)

        victims =
          if keep && keep > 0 do
            Enum.drop(victims, -keep)
          else
            victims
          end

        s =
          Enum.reduce(victims, s, fn msg, acc -> acc |> remove(msg) |> log({:d, elem(msg, 0)}) end)

        {length(victims), s}
    end
  end

  # ------------------------------------------------------------------- read

  @spec get(t() | :ets.tid(), pos_integer()) :: msg() | nil
  def get(%__MODULE__{tab: tab}, seq), do: get(tab, seq)

  def get(tab, seq) do
    case :ets.lookup(tab, seq) do
      [msg] -> msg
      [] -> nil
    end
  end

  @doc "Last message for a literal or wildcard subject."
  @spec last_by_subject(t(), binary()) :: msg() | nil
  def last_by_subject(%__MODULE__{} = s, subject) do
    if Subject.literal?(subject) do
      case :ets.lookup(s.by_subject, subject) do
        [{_, _, last}] -> get(s, last)
        [] -> nil
      end
    else
      s.by_subject
      |> :ets.tab2list()
      |> Enum.filter(fn {subj, _, _} -> Subject.matches?(subject, subj) end)
      |> Enum.map(fn {_, _, last} -> last end)
      |> case do
        [] -> nil
        seqs -> get(s, Enum.max(seqs))
      end
    end
  end

  @doc "First message at or after `seq` matching `subject` (literal or wildcard)."
  @spec next_by_subject(t(), binary(), pos_integer()) :: msg() | nil
  def next_by_subject(%__MODULE__{} = s, subject, seq) do
    if Subject.literal?(subject) do
      case :ets.next(s.idx, {subject, seq - 1}) do
        {^subject, found} -> get(s, found)
        _ -> nil
      end
    else
      next_matching(s.tab, seq - 1, Subject.tokens(subject))
    end
  end

  @doc "Walk forward from `after_seq` to the first message whose subject matches any of the token filters (`nil` = all)."
  @spec next_matching(:ets.tid(), non_neg_integer(), [[binary()]] | [binary()] | nil) ::
          msg() | nil
  def next_matching(tab, after_seq, filters) do
    case :ets.next(tab, after_seq) do
      :"$end_of_table" ->
        nil

      seq ->
        [{_, subject, _, _, _} = msg] = :ets.lookup(tab, seq)
        if matches_any?(filters, subject), do: msg, else: next_matching(tab, seq, filters)
    end
  end

  def matches_any?(nil, _subject), do: true
  def matches_any?([], _subject), do: true

  def matches_any?([f | _] = filters, subject) when is_binary(f),
    do: Enum.any?(filters, &Subject.matches?(&1, subject))

  def matches_any?(filters, subject),
    do: Enum.any?(filters, &Subject.matches?(&1, Subject.tokens(subject)))

  @doc "Number of messages after `seq` matching filters (for `num_pending`)."
  def count_after(tab, seq, filters) do
    count_after(tab, seq, filters, 0)
  end

  defp count_after(tab, seq, filters, acc) do
    case next_matching(tab, seq, filters) do
      nil -> acc
      {s, _, _, _, _} -> count_after(tab, s, filters, acc + 1)
    end
  end

  @doc "Last sequence per subject for subjects matching the filters (deliver_policy last_per_subject)."
  def last_per_subject(%__MODULE__{} = s, filters) do
    s.by_subject
    |> :ets.tab2list()
    |> Enum.filter(fn {subj, _, _} -> matches_any?(filters, subj) end)
    |> Enum.map(fn {_, _, last} -> last end)
    |> Enum.sort()
  end

  @doc "Subject → count map (for stream info `subjects`)."
  def subject_counts(%__MODULE__{} = s, filter \\ nil) do
    s.by_subject
    |> :ets.tab2list()
    |> Enum.filter(fn {subj, _, _} -> is_nil(filter) or Subject.matches?(filter, subj) end)
    |> Map.new(fn {subj, n, _} -> {subj, n} end)
  end

  def num_subjects(%__MODULE__{} = s), do: :ets.info(s.by_subject, :size)

  @doc "Messages (optionally filtered) in sequence order."
  def stream_msgs(%__MODULE__{} = s, filter \\ nil) do
    s.tab
    |> :ets.tab2list()
    |> Enum.filter(fn {_, subj, _, _, _} -> is_nil(filter) or Subject.matches?(filter, subj) end)
  end

  @doc "First message's timestamp (ns) or nil."
  def first_ts(%__MODULE__{msgs: 0}), do: nil

  def first_ts(%__MODULE__{} = s) do
    case get(s, s.first_seq) do
      {_, _, _, _, ts} -> ts
      nil -> nil
    end
  end

  @doc "Oldest sequence for a literal subject, or nil."
  def first_seq_for(%__MODULE__{} = s, subject) do
    case :ets.next(s.idx, {subject, 0}) do
      {^subject, seq} -> seq
      _ -> nil
    end
  end

  def count_for(%__MODULE__{} = s, subject) do
    case :ets.lookup(s.by_subject, subject) do
      [{_, n, _}] -> n
      [] -> 0
    end
  end

  @doc "Sequences that have been deleted inside [first_seq, last_seq]."
  def deleted(%__MODULE__{} = s) do
    if s.msgs == 0 or s.last_seq - s.first_seq + 1 == s.msgs do
      []
    else
      present = :ets.select(s.tab, [{{:"$1", :_, :_, :_, :_}, [], [:"$1"]}]) |> MapSet.new()
      for seq <- s.first_seq..s.last_seq, not MapSet.member?(present, seq), do: seq
    end
  end

  # ------------------------------------------------------------ file log

  defp log(%__MODULE__{file: nil} = s, _entry), do: s

  defp log(%__MODULE__{} = s, entry) do
    bin = :erlang.term_to_binary(entry)
    :ok = :file.write(s.file, <<byte_size(bin)::32, :erlang.crc32(bin)::32, bin::binary>>)
    s = %{s | log_entries: s.log_entries + 1, dirty: true}
    maybe_compact(s)
  end

  # Compact when the log holds many more entries than live messages.
  defp maybe_compact(%__MODULE__{log_entries: n, msgs: live} = s)
       when n > 10_000 and n > live * 3, do: compact(s)

  defp maybe_compact(s), do: s

  @doc "Rewrite the log from the live message set."
  def compact(%__MODULE__{file: nil} = s), do: s

  def compact(%__MODULE__{} = s) do
    :file.close(s.file)
    tmp = s.path <> ".tmp"
    {:ok, f} = :file.open(tmp, [:write, :binary, :raw, {:delayed_write, 1_048_576, 2000}])
    :ok = :file.write(f, <<@magic, s.last_seq::64>>)

    :ets.foldl(
      fn {seq, subject, hdrs, data, ts}, _ ->
        bin = :erlang.term_to_binary({:m, seq, subject, hdrs, data, ts})
        :file.write(f, <<byte_size(bin)::32, :erlang.crc32(bin)::32, bin::binary>>)
      end,
      nil,
      s.tab
    )

    :file.sync(f)
    :file.close(f)
    File.rename!(tmp, s.path)
    {:ok, file} = :file.open(s.path, [:append, :binary, :raw, {:delayed_write, 65_536, 200}])
    %{s | file: file, log_entries: s.msgs, dirty: false}
  end

  defp replay(s, path) do
    case File.read(path) do
      {:ok, <<@magic, last_seq::64, rest::binary>>} ->
        replay_entries(%{s | last_seq: last_seq, first_seq: last_seq + 1}, rest)

      {:ok, bin} ->
        replay_entries(s, bin)

      {:error, :enoent} ->
        s
    end
  end

  defp replay_entries(s, <<len::32, crc::32, bin::binary-size(len), rest::binary>>) do
    s =
      if :erlang.crc32(bin) == crc do
        apply_entry(s, :erlang.binary_to_term(bin))
      else
        s
      end

    replay_entries(%{s | log_entries: s.log_entries + 1}, rest)
  end

  # Truncated tail (crash mid-write): stop here.
  defp replay_entries(s, _), do: s

  defp apply_entry(s, {:m, seq, subject, hdrs, data, ts}) do
    s = insert(s, {seq, subject, hdrs, data, ts})
    %{s | last_seq: max(s.last_seq, seq), last_ts: ts}
  end

  defp apply_entry(s, {:d, seq}) do
    case :ets.lookup(s.tab, seq) do
      [msg] -> remove(s, msg)
      [] -> s
    end
  end

  defp apply_entry(s, {:l, seq}) when seq > s.last_seq do
    %{s | last_seq: seq, first_seq: if(s.msgs == 0, do: seq + 1, else: s.first_seq)}
  end

  defp apply_entry(s, {:p, :all}) do
    for t <- [s.tab, s.by_subject, s.idx], do: :ets.delete_all_objects(t)
    %{s | msgs: 0, bytes: 0, first_seq: s.last_seq + 1}
  end

  defp apply_entry(s, _), do: s
end
