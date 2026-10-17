defmodule Natsbeam.Subject do
  @moduledoc """
  Subject tokenisation, validation and wildcard matching.

  NATS subjects are dot-separated tokens. Subscriptions may use `*` to match a
  single token and `>` (only as the final token) to match one or more tokens.
  """

  @type tokens :: [binary()]

  @doc "Split a subject into its tokens."
  @spec tokens(binary()) :: tokens()
  def tokens(subject) when is_binary(subject), do: :binary.split(subject, ".", [:global])

  @doc """
  True for a subject that is syntactically valid for publishing: non-empty,
  no whitespace, no empty tokens. Wildcard characters are allowed (treated
  literally) unless `pedantic?` is set, mirroring nats-server behaviour.
  """
  @spec valid_publish?(binary(), boolean()) :: boolean()
  def valid_publish?(subject, pedantic? \\ false)
  def valid_publish?("", _), do: false

  # Non-pedantic (the hot path): a single byte scan, no tokenising and no
  # `:binary.match` (compiling its multi-pattern per call costs more than the
  # scan). Valid means no empty token (no leading/trailing dot, no "..") and
  # no whitespace.
  def valid_publish?(<<?., _::binary>>, false), do: false
  def valid_publish?(subject, false) when is_binary(subject), do: scan_publish(subject)

  def valid_publish?(subject, true) when is_binary(subject) do
    Enum.all?(tokens(subject), fn
      "" -> false
      "*" -> false
      ">" -> false
      tok -> not has_space?(tok)
    end)
  end

  defp scan_publish(<<>>), do: true
  defp scan_publish(<<?.>>), do: false
  defp scan_publish(<<?., ?., _::binary>>), do: false
  defp scan_publish(<<c, _::binary>>) when c in [?\s, ?\t, ?\r, ?\n], do: false
  defp scan_publish(<<_, rest::binary>>), do: scan_publish(rest)

  @doc "True for a syntactically valid subscription subject (wildcards permitted)."
  @spec valid_subscription?(binary()) :: boolean()
  def valid_subscription?(""), do: false

  def valid_subscription?(subject) when is_binary(subject) do
    toks = tokens(subject)
    valid_sub_tokens?(toks)
  end

  defp valid_sub_tokens?([]), do: true
  defp valid_sub_tokens?([">"]), do: true
  defp valid_sub_tokens?([">" | _]), do: false
  defp valid_sub_tokens?(["" | _]), do: false

  defp valid_sub_tokens?([tok | rest]) do
    cond do
      has_space?(tok) -> false
      tok != "*" and String.contains?(tok, ["*", ">"]) -> false
      true -> valid_sub_tokens?(rest)
    end
  end

  @doc "True when a subscription subject contains no wildcards."
  @spec literal?(binary()) :: boolean()
  def literal?(subject) when is_binary(subject) do
    not Enum.any?(tokens(subject), &(&1 == "*" or &1 == ">"))
  end

  @doc "Does the (possibly wildcarded) `pattern` match the literal `subject`?"
  @spec matches?(binary() | tokens(), binary() | tokens()) :: boolean()
  def matches?(pattern, subject) when is_binary(pattern), do: matches?(tokens(pattern), subject)
  def matches?(pattern, subject) when is_binary(subject), do: matches?(pattern, tokens(subject))

  def matches?([">"], [_ | _]), do: true
  def matches?([], []), do: true
  def matches?(["*" | prest], [_ | srest]), do: matches?(prest, srest)
  def matches?([t | prest], [t | srest]), do: matches?(prest, srest)
  def matches?(_, _), do: false

  @doc """
  Does pattern `a` intersect pattern `b`? Both may carry wildcards. Used for
  permission checks on wildcard subscriptions (a subscription on `foo.*` is
  allowed under permission `foo.>`, for example).
  """
  @spec intersects?(binary() | tokens(), binary() | tokens()) :: boolean()
  def intersects?(a, b) when is_binary(a), do: intersects?(tokens(a), b)
  def intersects?(a, b) when is_binary(b), do: intersects?(a, tokens(b))
  def intersects?([">"], [_ | _]), do: true
  def intersects?([_ | _], [">"]), do: true
  def intersects?([], []), do: true
  def intersects?(["*" | ar], [_ | br]), do: intersects?(ar, br)
  def intersects?([_ | ar], ["*" | br]), do: intersects?(ar, br)
  def intersects?([t | ar], [t | br]), do: intersects?(ar, br)
  def intersects?(_, _), do: false

  @doc """
  Is pattern `sub` entirely contained by pattern `perm`? Every subject that
  `sub` could match is also matched by `perm`. This is what nats-server uses
  to decide whether a wildcard subscription is permitted by an allow list.
  """
  @spec subset?(binary() | tokens(), binary() | tokens()) :: boolean()
  def subset?(sub, perm) when is_binary(sub), do: subset?(tokens(sub), perm)
  def subset?(sub, perm) when is_binary(perm), do: subset?(sub, tokens(perm))
  def subset?([_ | _], [">"]), do: true
  def subset?([], []), do: true
  def subset?([">"], _), do: false
  def subset?([_ | sr], ["*" | pr]), do: subset?(sr, pr)
  def subset?([t | sr], [t | pr]) when t != "*", do: subset?(sr, pr)
  def subset?(_, _), do: false

  defp has_space?(tok) do
    :binary.match(tok, [" ", "\t", "\r", "\n"]) != :nomatch
  end
end
