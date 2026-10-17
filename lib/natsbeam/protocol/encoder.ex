defmodule Natsbeam.Protocol.Encoder do
  @moduledoc "Builders for server-to-client protocol frames. All return iodata."

  @spec info(map()) :: iodata()
  def info(map), do: ["INFO ", Jason.encode_to_iodata!(map), "\r\n"]

  @spec msg(binary(), binary(), binary() | nil, iodata()) :: iodata()
  def msg(subject, sid, nil, payload),
    do: [
      "MSG ",
      subject,
      " ",
      sid,
      " ",
      Integer.to_string(len(payload)),
      "\r\n",
      payload,
      "\r\n"
    ]

  def msg(subject, sid, reply, payload),
    do: [
      "MSG ",
      subject,
      " ",
      sid,
      " ",
      reply,
      " ",
      Integer.to_string(len(payload)),
      "\r\n",
      payload,
      "\r\n"
    ]

  defp len(payload) when is_binary(payload), do: byte_size(payload)
  defp len(payload), do: IO.iodata_length(payload)

  @doc """
  HMSG frame. `headers` must be a complete header block
  (`"NATS/1.0\\r\\n...\\r\\n\\r\\n"`).
  """
  @spec hmsg(binary(), binary(), binary() | nil, binary(), iodata()) :: iodata()
  def hmsg(subject, sid, reply, headers, payload) do
    hlen = byte_size(headers)
    total = hlen + len(payload)

    [
      "HMSG ",
      subject,
      " ",
      sid,
      if(reply, do: [" ", reply], else: []),
      " ",
      Integer.to_string(hlen),
      " ",
      Integer.to_string(total),
      "\r\n",
      headers,
      payload,
      "\r\n"
    ]
  end

  def ok, do: "+OK\r\n"
  def ping, do: "PING\r\n"
  def pong, do: "PONG\r\n"
  def err(reason), do: ["-ERR '", reason, "'\r\n"]

  @doc "Header block for a no-responders (503) status message."
  def no_responders_headers, do: "NATS/1.0 503\r\n\r\n"
end
