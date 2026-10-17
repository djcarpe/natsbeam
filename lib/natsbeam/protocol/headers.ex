defmodule Natsbeam.Protocol.Headers do
  @moduledoc """
  Encode/decode NATS header blocks (`NATS/1.0[ status[ description]]\\r\\nKey: Value\\r\\n...\\r\\n\\r\\n`).
  """

  @type t :: %{
          status: binary() | nil,
          description: binary() | nil,
          fields: [{binary(), binary()}]
        }

  @spec decode(binary()) :: {:ok, t()} | :error
  def decode(block) when is_binary(block) do
    case String.split(block, "\r\n") do
      ["NATS/1.0" <> inline | rest] ->
        {status, desc} =
          case String.split(String.trim(inline), " ", parts: 2) do
            [""] -> {nil, nil}
            [s] -> {s, nil}
            [s, d] -> {s, d}
          end

        fields =
          rest
          |> Enum.reject(&(&1 == ""))
          |> Enum.flat_map(fn line ->
            case String.split(line, ":", parts: 2) do
              [k, v] -> [{String.trim(k), String.trim(v)}]
              _ -> []
            end
          end)

        {:ok, %{status: status, description: desc, fields: fields}}

      _ ->
        :error
    end
  end

  @spec encode([{binary(), binary()}] | map(), binary() | nil, binary() | nil) :: binary()
  def encode(fields, status \\ nil, description \\ nil) do
    first =
      [
        "NATS/1.0",
        if(status, do: [" ", status], else: []),
        if(description, do: [" ", description], else: [])
      ]

    body =
      fields
      |> Enum.map(fn {k, v} -> [to_string(k), ": ", to_string(v), "\r\n"] end)

    IO.iodata_to_binary([first, "\r\n", body, "\r\n"])
  end

  @doc "Fetch a header value (case-insensitive key) from a decoded header map."
  def get(%{fields: fields}, key) do
    lk = String.downcase(key)
    Enum.find_value(fields, fn {k, v} -> if String.downcase(k) == lk, do: v end)
  end
end
