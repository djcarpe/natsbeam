defmodule Natsbeam.Local do
  @moduledoc """
  In-VM NATS participation for Elixir processes: subscribe, publish and request
  on an account without opening a socket. Subscriptions are owned by the
  calling process and vanish when it exits.

  Delivered messages: `{:natsbeam_msg, sid, subject, reply, headers | nil, payload}`.
  """
  alias Natsbeam.{Account, Config}

  @type sid :: reference()

  defp account_name(opts),
    do: to_string(Keyword.get(opts, :account) || Config.get([:auth, :default_account]))

  @doc "Subscribe the calling process (or `:pid`). Options: `:account`, `:queue`, `:pid`, `:sid`."
  @spec subscribe(binary(), keyword()) :: {:ok, sid()} | {:error, term()}
  def subscribe(subject, opts \\ []) do
    pid = Keyword.get(opts, :pid, self())
    sid = Keyword.get(opts, :sid, make_ref())
    queue = Keyword.get(opts, :queue)
    name = account_name(opts)

    with true <- Natsbeam.Subject.valid_subscription?(subject) || {:error, :invalid_subject},
         {:ok, apid, _} <- Account.join(name, pid, %{kind: :local}),
         :ok <- Account.subscribe(apid, pid, sid, subject, queue) do
      {:ok, sid}
    end
  end

  @doc "Remove a subscription made by the calling process (or `:pid`)."
  def unsubscribe(sid, opts \\ []) do
    pid = Keyword.get(opts, :pid, self())

    case Account.lookup(account_name(opts)) do
      {:ok, apid, _} -> Account.unsubscribe(apid, pid, sid) && :ok
      :error -> :ok
    end
  end

  @doc "Publish. Options: `:account`, `:reply`, `:headers` (list of `{k, v}` or raw block)."
  @spec publish(binary(), iodata(), keyword()) :: {:ok, non_neg_integer()} | {:error, term()}
  def publish(subject, payload, opts \\ []) do
    headers =
      case Keyword.get(opts, :headers) do
        nil -> nil
        block when is_binary(block) -> block
        fields -> Natsbeam.Protocol.Headers.encode(fields)
      end

    if Natsbeam.Subject.valid_publish?(subject) do
      {:ok, _apid, sublist} = Account.ensure(account_name(opts))

      n =
        Account.publish(
          sublist,
          subject,
          Keyword.get(opts, :reply),
          headers,
          IO.iodata_to_binary(payload),
          self(),
          true
        )

      {:ok, n}
    else
      {:error, :invalid_subject}
    end
  end

  @doc """
  Request/reply with a temporary inbox. Options: `:account`, `:timeout`
  (default 2000ms), `:headers`. Returns `{:ok, %{subject:, headers:, payload:}}`,
  `{:error, :no_responders}` or `{:error, :timeout}`.
  """
  def request(subject, payload, opts \\ []) do
    timeout = Keyword.get(opts, :timeout, 2_000)
    inbox = "_INBOX." <> Base.url_encode64(:crypto.strong_rand_bytes(12), padding: false)
    {:ok, sid} = subscribe(inbox, opts)

    try do
      case publish(subject, payload, Keyword.put(opts, :reply, inbox)) do
        {:ok, 0} ->
          {:error, :no_responders}

        {:ok, _} ->
          receive do
            {:natsbeam_msg, ^sid, subj, reply, headers, body} ->
              {:ok,
               %{
                 subject: subj,
                 headers: headers && decode_headers(headers),
                 payload: body,
                 reply: reply
               }}
          after
            timeout -> {:error, :timeout}
          end

        err ->
          err
      end
    after
      unsubscribe(sid, opts)
    end
  end

  defp decode_headers(block) do
    case Natsbeam.Protocol.Headers.decode(block) do
      {:ok, h} -> h
      :error -> nil
    end
  end
end
