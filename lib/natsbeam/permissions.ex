defmodule Natsbeam.Permissions do
  @moduledoc """
  Publish/subscribe permission sets, in the same shape nats-server uses.

      %Natsbeam.Permissions{
        publish: %{allow: ["orders.>"], deny: ["orders.admin.*"]},
        subscribe: %{allow: ["orders.> workers"], deny: []},
        response: %{max: 1, ttl: 0}
      }

  Subscribe entries may carry a queue filter after a space (`"subject queue"`),
  which only applies to queue subscriptions on a matching queue name.
  An empty/absent allow list means "allow everything not denied".
  """
  alias Natsbeam.Subject

  defstruct publish: %{allow: [], deny: []}, subscribe: %{allow: [], deny: []}, response: nil
  @type t :: %__MODULE__{}

  @doc "Build from a loosely-typed map (JWT `nats.pub`/`nats.sub`/`nats.resp` or config)."
  @spec from_map(map() | nil) :: t() | nil
  def from_map(nil), do: nil

  def from_map(map) when is_map(map) do
    pub = fetch(map, [:publish, "publish", :pub, "pub"])
    sub = fetch(map, [:subscribe, "subscribe", :sub, "sub"])

    resp =
      fetch(map, [:response, "response", :resp, "resp"]) ||
        fetch(map, [:allow_responses, "allow_responses"])

    %__MODULE__{
      publish: lists(pub),
      subscribe: lists(sub),
      response: response(resp)
    }
  end

  defp fetch(map, keys), do: Enum.find_value(keys, &Map.get(map, &1))

  # Accept a list (allow only), a map with allow/deny, or nil.
  defp lists(nil), do: %{allow: [], deny: []}
  defp lists(list) when is_list(list), do: %{allow: list, deny: []}

  defp lists(map) when is_map(map) do
    %{
      allow: List.wrap(fetch(map, [:allow, "allow"]) || []),
      deny: List.wrap(fetch(map, [:deny, "deny"]) || [])
    }
  end

  defp response(nil), do: nil
  defp response(false), do: nil
  defp response(true), do: %{max: 1, ttl: 0}

  defp response(map) when is_map(map) do
    %{
      max: fetch(map, [:max, "max"]) || 1,
      ttl: normalize_ttl(fetch(map, [:ttl, "ttl"]) || 0)
    }
  end

  # JWT ttl is nanoseconds (Go duration); config may be ms. Treat large values as ns.
  defp normalize_ttl(ttl) when is_integer(ttl) and ttl > 1_000_000_000, do: div(ttl, 1_000_000)
  defp normalize_ttl(ttl) when is_integer(ttl), do: ttl
  defp normalize_ttl(_), do: 0

  @doc "May this identity publish to the literal `subject`?"
  @spec can_publish?(t() | nil, binary()) :: boolean()
  def can_publish?(nil, _), do: true

  def can_publish?(%__MODULE__{publish: %{allow: [], deny: []}}, _subject), do: true

  def can_publish?(%__MODULE__{publish: %{allow: allow, deny: deny}}, subject) do
    # Tokenise the subject once rather than once per allow/deny entry.
    toks = Subject.tokens(subject)
    denied? = Enum.any?(deny, &Subject.matches?(&1, toks))
    allowed? = allow == [] or Enum.any?(allow, &Subject.matches?(&1, toks))
    allowed? and not denied?
  end

  @doc "May this identity subscribe to `subject` (which may hold wildcards), optionally in `queue`?"
  @spec can_subscribe?(t() | nil, binary(), binary() | nil) :: boolean()
  def can_subscribe?(nil, _, _), do: true

  def can_subscribe?(%__MODULE__{subscribe: %{allow: allow, deny: deny}}, subject, queue) do
    denied? = Enum.any?(deny, &deny_entry_hits?(&1, subject, queue))
    allowed? = allow == [] or Enum.any?(allow, &allow_entry_covers?(&1, subject, queue))
    allowed? and not denied?
  end

  defp split_entry(entry) do
    case String.split(entry, " ", parts: 2, trim: true) do
      [s] -> {s, nil}
      [s, q] -> {s, q}
    end
  end

  # An allow entry covers the subscription when the subscription's subject
  # space is a subset of the entry's, and (if the entry names a queue) the
  # subscription is a queue sub whose group matches that queue pattern.
  defp allow_entry_covers?(entry, subject, queue) do
    {es, eq} = split_entry(entry)

    cond do
      not Subject.subset?(subject, es) -> false
      is_nil(eq) -> true
      is_nil(queue) -> false
      true -> Subject.matches?(eq, queue)
    end
  end

  # A deny entry hits when the subscription overlaps the denied subject space
  # and (if the entry names a queue) the queue matches; a deny without a queue
  # hits plain and queue subs alike.
  defp deny_entry_hits?(entry, subject, queue) do
    {es, eq} = split_entry(entry)

    cond do
      not Subject.intersects?(subject, es) -> false
      is_nil(eq) -> true
      is_nil(queue) -> false
      true -> Subject.matches?(eq, queue)
    end
  end

  @doc "Merge: entries in `override` replace those in `base` when present."
  def merge(nil, other), do: other
  def merge(base, nil), do: base
  def merge(%__MODULE__{} = _base, %__MODULE__{} = override), do: override
end
