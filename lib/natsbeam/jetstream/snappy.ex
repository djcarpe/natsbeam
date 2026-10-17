defmodule Natsbeam.JetStream.Snappy do
  @moduledoc """
  Minimal Snappy framing-format codec (what nats clients expect for stream
  snapshots; S2 readers accept it). Encoding emits uncompressed frames, which
  is enough for correctness; decoding understands both uncompressed and
  compressed frames so files from other producers restore too.
  """
  import Bitwise

  @stream_id <<0xFF, 0x06, 0x00, 0x00, "sNaPpY">>
  @max_chunk 65_536

  @doc "Wrap `data` in a Snappy framed stream (uncompressed frames)."
  @spec encode(binary()) :: binary()
  def encode(data) do
    frames =
      for <<chunk::binary-size(min(@max_chunk, max(byte_size(data), 1))) <- data>>,
        do: frame(chunk)

    rest_len = rem(byte_size(data), @max_chunk)

    tail =
      if rest_len > 0 and byte_size(data) > @max_chunk,
        do: frame(binary_part(data, byte_size(data) - rest_len, rest_len)),
        else: []

    frames = if byte_size(data) <= @max_chunk, do: [frame(data)], else: frames ++ [tail]
    IO.iodata_to_binary([@stream_id | frames])
  end

  defp frame(chunk) do
    crc = masked_crc(chunk)
    len = byte_size(chunk) + 4
    <<0x01, len::little-24, crc::little-32, chunk::binary>>
  end

  @doc "True when `data` starts with the Snappy stream identifier."
  def framed?(<<0xFF, 0x06, 0x00, 0x00, "sNaPpY", _::binary>>), do: true
  def framed?(_), do: false

  @doc "Decode a framed stream back into the original bytes."
  @spec decode(binary()) :: {:ok, binary()} | {:error, term()}
  def decode(<<0xFF, 0x06, 0x00, 0x00, "sNaPpY", rest::binary>>), do: frames(rest, [])
  def decode(_), do: {:error, :not_framed}

  defp frames(<<>>, acc), do: {:ok, IO.iodata_to_binary(Enum.reverse(acc))}

  defp frames(<<type, len::little-24, body::binary-size(len), rest::binary>>, acc) do
    case type do
      0x01 ->
        <<_crc::little-32, chunk::binary>> = body
        frames(rest, [chunk | acc])

      0x00 ->
        <<_crc::little-32, compressed::binary>> = body

        case decompress_block(compressed) do
          {:ok, chunk} -> frames(rest, [chunk | acc])
          err -> err
        end

      0xFF ->
        frames(rest, acc)

      t when t >= 0x80 ->
        frames(rest, acc)

      _ ->
        {:error, :unsupported_frame}
    end
  end

  defp frames(_, _), do: {:error, :truncated}

  # -- Snappy block format ----------------------------------------------

  @doc false
  def decompress_block(data) do
    {_len, rest} = varint(data, 0, 0)
    {:ok, blocks(rest, <<>>)}
  rescue
    _ -> {:error, :corrupt_block}
  end

  defp varint(<<b, rest::binary>>, shift, acc) when b >= 0x80,
    do: varint(rest, shift + 7, acc ||| (b &&& 0x7F) <<< shift)

  defp varint(<<b, rest::binary>>, shift, acc), do: {acc ||| b <<< shift, rest}

  defp blocks(<<>>, out), do: out

  defp blocks(<<tag, rest::binary>>, out) do
    case tag &&& 0x03 do
      0 ->
        {len, rest} =
          case tag >>> 2 do
            n when n < 60 -> {n + 1, rest}
            60 -> (<<l, r::binary>> = rest) && {l + 1, r}
            61 -> (<<l::little-16, r::binary>> = rest) && {l + 1, r}
            62 -> (<<l::little-24, r::binary>> = rest) && {l + 1, r}
            63 -> (<<l::little-32, r::binary>> = rest) && {l + 1, r}
          end

        <<literal::binary-size(len), rest::binary>> = rest
        blocks(rest, out <> literal)

      1 ->
        <<off_hi, rest::binary>> = rest
        len = (tag >>> 2 &&& 0x07) + 4
        offset = (tag >>> 5 &&& 0x07) <<< 8 ||| off_hi
        blocks(rest, copy(out, offset, len))

      2 ->
        <<offset::little-16, rest::binary>> = rest
        blocks(rest, copy(out, offset, (tag >>> 2) + 1))

      3 ->
        <<offset::little-32, rest::binary>> = rest
        blocks(rest, copy(out, offset, (tag >>> 2) + 1))
    end
  end

  defp copy(out, offset, len) do
    start = byte_size(out) - offset
    # Overlapping copies repeat the pattern.
    Enum.reduce(1..len, out, fn i, acc ->
      acc <> binary_part(acc, start + i - 1, 1)
    end)
  end

  # -- CRC-32C (Castagnoli), masked as Snappy requires ----------------------

  @doc false
  def masked_crc(data) do
    c = crc32c(data)
    (c >>> 15 ||| c <<< 17) + 0xA282EAD8 &&& 0xFFFFFFFF
  end

  @table (for i <- 0..255 do
            Enum.reduce(1..8, i, fn _, c ->
              if (c &&& 1) == 1, do: bxor(c >>> 1, 0x82F63B78), else: c >>> 1
            end)
          end)
         |> List.to_tuple()

  @doc false
  def crc32c(data), do: crc32c(data, 0xFFFFFFFF)
  defp crc32c(<<>>, crc), do: bxor(crc, 0xFFFFFFFF)

  defp crc32c(<<b, rest::binary>>, crc) do
    crc32c(rest, bxor(elem(@table, bxor(crc, b) &&& 0xFF), crc >>> 8))
  end
end
