defmodule Natsbeam.Sublist do
  @moduledoc """
  ETS-backed subscription index for one account.

  Four public tables (so publishers match concurrently without touching the
  account process; writes are serialised through `Natsbeam.Account`):

    * `children` (set): `{{rev_prefix, token}, count}` — trie edges, refcounted
    * `subs` (bag): `rev_tokens => {pid, sid, queue}` — subscriptions at a node
    * `by_pid` (bag): `pid => {sid, rev_tokens, queue}` — for cleanup/lookup
    * `meta` (set): `{:remote_peers, [pid]}`, the account processes on other
      nodes (maintained by `Natsbeam.Account` from `:pg` notifications)

  plus `gen`, an `:atomics` counter bumped by every write and by every change
  of the remote peers, so publishers can keep a per-process cache of matches
  and peers and drop it when interest changes (one atomic read per publish).

  Token lists are stored reversed so that descending the trie is a cheap prepend.
  """
  alias Natsbeam.Subject

  defstruct [:children, :subs, :by_pid, :meta, :gen, :account]
  @type t :: %__MODULE__{}
  @type match :: {pid(), sid :: term(), queue :: binary() | nil}

  def new(account \\ nil) do
    opts = [:public, read_concurrency: true, write_concurrency: true]

    meta = :ets.new(:natsbeam_meta, [:set | opts])
    :ets.insert(meta, {:remote_peers, []})

    %__MODULE__{
      account: account,
      children: :ets.new(:natsbeam_children, [:set | opts]),
      subs: :ets.new(:natsbeam_subs, [:bag | opts]),
      by_pid: :ets.new(:natsbeam_by_pid, [:bag | opts]),
      meta: meta,
      gen: :atomics.new(1, signed: false)
    }
  end

  def delete(%__MODULE__{} = t) do
    for tab <- [t.children, t.subs, t.by_pid, t.meta], do: :ets.delete(tab)
    :ok
  end

  @doc """
  Interest generation: incremented by every insert/remove and by every change
  of the remote peers. A publisher that caches `match/2` results and
  `remote_peers/1` compares this to the generation it cached under.
  """
  @spec generation(t()) :: non_neg_integer()
  def generation(%__MODULE__{gen: gen}), do: :atomics.get(gen, 1)

  @doc "Account processes for this account on other nodes (see `Natsbeam.Account`)."
  @spec remote_peers(t()) :: [pid()]
  def remote_peers(%__MODULE__{meta: meta}), do: :ets.lookup_element(meta, :remote_peers, 2)

  @doc false
  def set_remote_peers(%__MODULE__{meta: meta} = t, pids) when is_list(pids) do
    :ets.insert(meta, {:remote_peers, pids})
    bump(t)
    :ok
  end

  defp bump(%__MODULE__{gen: gen}), do: :atomics.add(gen, 1, 1)

  @doc "Insert a subscription. Idempotent for the same `{pid, sid}` (replaces)."
  @spec insert(t(), pid(), term(), binary(), binary() | nil) :: :ok
  def insert(t, pid, sid, subject, queue) do
    remove(t, pid, sid)
    tokens = Subject.tokens(subject)
    add_edges(t, [], tokens)
    rev = Enum.reverse(tokens)
    :ets.insert(t.subs, {rev, {pid, sid, queue}})
    :ets.insert(t.by_pid, {pid, {sid, rev, queue}})
    bump(t)
    :ok
  end

  @doc "Remove one subscription by `{pid, sid}`. Returns `true` if it existed."
  @spec remove(t(), pid(), term()) :: boolean()
  def remove(t, pid, sid) do
    case :ets.match_object(t.by_pid, {pid, {sid, :_, :_}}) do
      [] ->
        false

      entries ->
        for {^pid, {^sid, rev, queue}} = e <- entries do
          :ets.delete_object(t.by_pid, e)
          :ets.delete_object(t.subs, {rev, {pid, sid, queue}})
          drop_edges(t, [], Enum.reverse(rev))
        end

        bump(t)
        true
    end
  end

  @doc "Remove every subscription held by `pid`. Returns the number removed."
  @spec remove_all(t(), pid()) :: non_neg_integer()
  def remove_all(t, pid) do
    entries = :ets.lookup(t.by_pid, pid)

    for {^pid, {sid, rev, queue}} <- entries do
      :ets.delete_object(t.subs, {rev, {pid, sid, queue}})
      drop_edges(t, [], Enum.reverse(rev))
    end

    :ets.delete(t.by_pid, pid)
    if entries != [], do: bump(t)
    length(entries)
  end

  @doc "All subscriptions held by `pid` as `{sid, subject, queue}`."
  def subscriptions(t, pid) do
    for {^pid, {sid, rev, queue}} <- :ets.lookup(t.by_pid, pid) do
      {sid, Enum.join(Enum.reverse(rev), "."), queue}
    end
  end

  @doc """
  Interest weight for an exact `{subject, queue}`: the number of subscriptions
  on that literal subject in that queue group (nil = plain), excluding those
  owned by `exclude` (so a link never counts the interest it created itself).
  `exclude` may also be a function of `{pid, sid}` returning true for the
  subscriptions to leave out.

  Gateway subscriptions (`{:link, {:gw, ...}}` sids, see `Natsbeam.Gateway`)
  never count: nats-server does not propagate a remote cluster's interest over
  routes or leafnodes.
  """
  @spec weight(t(), binary(), binary() | nil, pid() | (term() -> boolean()) | nil) ::
          non_neg_integer()
  def weight(t, subject, queue, exclude \\ nil) do
    rev = Enum.reverse(Subject.tokens(subject))
    skip? = excluder(exclude)

    Enum.count(:ets.lookup(t.subs, rev), fn {_, {pid, sid, q}} ->
      q == queue and not skip?.({pid, sid}) and not gateway_sid?(sid)
    end)
  end

  defp excluder(nil), do: fn _ -> false end
  defp excluder(fun) when is_function(fun, 1), do: fun
  defp excluder(pid), do: fn {p, _} -> p == pid end

  @doc "Every distinct `{subject, queue}` with its weight, excluding `exclude`'s own subscriptions."
  @spec interest(t(), pid() | (term() -> boolean()) | nil) ::
          [{binary(), binary() | nil, pos_integer()}]
  def interest(t, exclude \\ nil) do
    skip? = excluder(exclude)

    t.subs
    |> :ets.tab2list()
    |> Enum.reject(fn {_, {pid, sid, _}} -> skip?.({pid, sid}) or gateway_sid?(sid) end)
    |> Enum.group_by(fn {rev, {_, _, q}} -> {rev, q} end)
    |> Enum.map(fn {{rev, q}, subs} -> {Enum.join(Enum.reverse(rev), "."), q, length(subs)} end)
  end

  @doc """
  Like `weight/4` but counting only subscriptions made on this server by
  clients and in-VM processes: every server-to-server link subscription
  (`{:link, ...}` sid) is excluded. Gateways advertise this view.
  """
  @spec local_weight(t(), binary(), binary() | nil) :: non_neg_integer()
  def local_weight(t, subject, queue) do
    rev = Enum.reverse(Subject.tokens(subject))

    Enum.count(:ets.lookup(t.subs, rev), fn {_, {_, sid, q}} ->
      q == queue and not link_sid?(sid)
    end)
  end

  @doc "Like `interest/2` but without any link subscriptions (see `local_weight/3`)."
  @spec local_interest(t()) :: [{binary(), binary() | nil, pos_integer()}]
  def local_interest(t) do
    t.subs
    |> :ets.tab2list()
    |> Enum.reject(fn {_, {_, sid, _}} -> link_sid?(sid) end)
    |> Enum.group_by(fn {rev, {_, _, q}} -> {rev, q} end)
    |> Enum.map(fn {{rev, q}, subs} -> {Enum.join(Enum.reverse(rev), "."), q, length(subs)} end)
  end

  @doc "Does the index hold at least one non-link subscription?"
  @spec local_subscriptions?(t()) :: boolean()
  def local_subscriptions?(t) do
    :ets.foldl(fn {_, {_, sid, _}}, acc -> acc or not link_sid?(sid) end, false, t.subs)
  end

  @doc false
  def link_sid?({:link, _}), do: true
  def link_sid?(_), do: false

  @doc false
  def gateway_sid?({:link, {:gw, _, _}}), do: true
  def gateway_sid?(_), do: false

  @doc "Total number of subscriptions in the index."
  def count(t), do: :ets.info(t.subs, :size)

  @doc "Find every subscription whose subject matches the literal `subject`."
  @spec match(t(), binary()) :: [match()]
  def match(t, subject) when is_binary(subject) do
    collect(t, [], Subject.tokens(subject), [])
  end

  @doc "Is there a subscription on exactly this literal subject (not just a wildcard match)?"
  @spec exact_interest?(t(), binary()) :: boolean()
  def exact_interest?(t, subject) do
    :ets.lookup(t.subs, Enum.reverse(Subject.tokens(subject))) != []
  end

  @doc "Is there any interest (at least one matching subscription)?"
  @spec interest?(t(), binary()) :: boolean()
  def interest?(t, subject), do: match(t, subject) != []

  defp collect(t, prefix, [], acc), do: node_subs(t, prefix) ++ acc

  defp collect(t, prefix, [tok | rest], acc) do
    acc = if edge?(t, prefix, ">"), do: node_subs(t, [">" | prefix]) ++ acc, else: acc
    acc = if edge?(t, prefix, "*"), do: collect(t, ["*" | prefix], rest, acc), else: acc

    if tok != "*" and tok != ">" and edge?(t, prefix, tok) do
      collect(t, [tok | prefix], rest, acc)
    else
      acc
    end
  end

  defp node_subs(t, rev), do: for({_, m} <- :ets.lookup(t.subs, rev), do: m)

  defp edge?(t, prefix, tok), do: :ets.member(t.children, {prefix, tok})

  defp add_edges(_t, _prefix, []), do: :ok

  defp add_edges(t, prefix, [tok | rest]) do
    :ets.update_counter(t.children, {prefix, tok}, 1, {{prefix, tok}, 0})
    add_edges(t, [tok | prefix], rest)
  end

  defp drop_edges(_t, _prefix, []), do: :ok

  defp drop_edges(t, prefix, [tok | rest]) do
    case :ets.update_counter(t.children, {prefix, tok}, -1, {{prefix, tok}, 0}) do
      n when n <= 0 -> :ets.delete(t.children, {prefix, tok})
      _ -> :ok
    end

    drop_edges(t, [tok | prefix], rest)
  end
end
