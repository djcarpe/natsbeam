defmodule Natsbeam.NKeys do
  @moduledoc """
  NATS NKeys: Ed25519 keys encoded with a role prefix, a CRC-16 checksum and
  RFC 4648 base32 (no padding). Compatible with `nk` and the Go `nkeys` library.

      iex> {seed, pub} = Natsbeam.NKeys.generate(:user)
      iex> String.starts_with?(seed, "SU") and String.starts_with?(pub, "U")
      true
  """
  import Bitwise

  @prefixes %{
    seed: 18 <<< 3,
    private: 15 <<< 3,
    server: 13 <<< 3,
    cluster: 2 <<< 3,
    operator: 14 <<< 3,
    account: 0,
    user: 20 <<< 3,
    curve: 23 <<< 3
  }
  @byte_to_type Map.new(@prefixes, fn {k, v} -> {v, k} end)
  @public_types [:server, :cluster, :operator, :account, :user, :curve]

  @type type :: :server | :cluster | :operator | :account | :user | :curve
  @type public_key :: binary()
  @type seed :: binary()

  @doc "Generate a new keypair; returns `{encoded_seed, encoded_public_key}`."
  @spec generate(type()) :: {seed(), public_key()}
  def generate(type) when type in @public_types do
    raw_seed = :crypto.strong_rand_bytes(32)
    {pub, _priv} = :crypto.generate_key(:eddsa, :ed25519, raw_seed)
    {encode_seed(type, raw_seed), encode(type, pub)}
  end

  @doc "Encode a raw 32-byte public key with the given role prefix."
  @spec encode(type(), binary()) :: public_key()
  def encode(type, <<raw::binary-size(32)>>) when type in @public_types do
    b32(<<Map.fetch!(@prefixes, type)>> <> raw)
  end

  @doc "Encode a raw 32-byte Ed25519 seed as an NKey seed (`S...`)."
  @spec encode_seed(type(), binary()) :: seed()
  def encode_seed(type, <<raw::binary-size(32)>>) when type in @public_types do
    pb = Map.fetch!(@prefixes, type)
    b1 = @prefixes.seed ||| pb >>> 5
    b2 = (pb &&& 31) <<< 3
    b32(<<b1, b2>> <> raw)
  end

  @doc "Decode an encoded public key into `{:ok, type, raw32}`."
  @spec decode(binary()) :: {:ok, type(), binary()} | {:error, term()}
  def decode(encoded) when is_binary(encoded) do
    with {:ok, <<prefix, raw::binary-size(32)>>} <- unb32(encoded),
         {:ok, type} <- Map.fetch(@byte_to_type, prefix),
         true <- type in @public_types do
      {:ok, type, raw}
    else
      _ -> {:error, :invalid_public_key}
    end
  end

  @doc "Decode an encoded seed into `{:ok, type, raw_seed32}`."
  @spec decode_seed(binary()) :: {:ok, type(), binary()} | {:error, term()}
  def decode_seed(encoded) when is_binary(encoded) do
    with {:ok, <<b1, b2, raw::binary-size(32)>>} <- unb32(encoded),
         true <- (b1 &&& 0xF8) == @prefixes.seed,
         pb = (b1 &&& 7) <<< 5 ||| (b2 &&& 0xF8) >>> 3,
         {:ok, type} <- Map.fetch(@byte_to_type, pb),
         true <- type in @public_types do
      {:ok, type, raw}
    else
      _ -> {:error, :invalid_seed}
    end
  end

  @doc "Derive the encoded public key from an encoded seed."
  @spec public_from_seed(seed()) :: {:ok, public_key()} | {:error, term()}
  def public_from_seed(seed) do
    with {:ok, type, raw} <- decode_seed(seed) do
      {pub, _} = :crypto.generate_key(:eddsa, :ed25519, raw)
      {:ok, encode(type, pub)}
    end
  end

  @doc "Sign `message` with an encoded seed. Returns the raw 64-byte signature."
  @spec sign(seed(), binary()) :: {:ok, binary()} | {:error, term()}
  def sign(seed, message) do
    with {:ok, _type, raw} <- decode_seed(seed) do
      {:ok, :crypto.sign(:eddsa, :none, message, [raw, :ed25519])}
    end
  end

  @doc "Verify a raw signature over `message` against an encoded public key."
  @spec verify(public_key(), binary(), binary()) :: boolean()
  def verify(public_key, message, signature) when is_binary(signature) do
    case decode(public_key) do
      {:ok, _type, raw} -> :crypto.verify(:eddsa, :none, message, signature, [raw, :ed25519])
      _ -> false
    end
  end

  @doc "True when `key` is a validly encoded public key of the given type (or any type)."
  @spec valid_public?(binary(), type() | :any) :: boolean()
  def valid_public?(key, type \\ :any)

  def valid_public?(key, type) when is_binary(key) do
    case decode(key) do
      {:ok, t, _} -> type == :any or t == type
      _ -> false
    end
  end

  def valid_public?(_, _), do: false

  @doc "Type of an encoded public key, or nil."
  def type_of(key) do
    case decode(key) do
      {:ok, t, _} -> t
      _ -> nil
    end
  end

  # -- CRC16 (XMODEM, poly 0x1021, init 0), as used by nkeys -----------------

  @doc false
  def crc16(data), do: crc16(data, 0)
  defp crc16(<<>>, crc), do: crc

  defp crc16(<<b, rest::binary>>, crc) do
    crc = bxor(crc, b <<< 8) &&& 0xFFFF

    crc =
      Enum.reduce(1..8, crc, fn _, c ->
        if (c &&& 0x8000) != 0, do: bxor(c <<< 1, 0x1021) &&& 0xFFFF, else: c <<< 1 &&& 0xFFFF
      end)

    crc16(rest, crc)
  end

  defp b32(data) do
    crc = crc16(data)
    Base.encode32(data <> <<crc::little-16>>, padding: false)
  end

  defp unb32(encoded) do
    with {:ok, bin} <- Base.decode32(encoded, padding: false),
         size when size > 2 <- byte_size(bin) do
      body = binary_part(bin, 0, size - 2)
      <<crc::little-16>> = binary_part(bin, size - 2, 2)
      if crc16(body) == crc, do: {:ok, body}, else: {:error, :bad_checksum}
    else
      _ -> {:error, :invalid_encoding}
    end
  end
end
