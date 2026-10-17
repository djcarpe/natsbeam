defmodule Natsbeam.JWT do
  @moduledoc """
  Decode and verify NATS JWTs (`alg: ed25519-nkey`), as produced by `nsc`.

  A NATS JWT is `base64url(header).base64url(claims).base64url(signature)`
  where the signature is Ed25519 over the ASCII `header.claims` string, made
  with the issuer's NKey. The issuer is carried in the `iss` claim.
  """
  alias Natsbeam.NKeys

  @type claims :: map()

  @doc """
  Decode a JWT **without** verifying it. Returns `{:ok, claims, header}`.
  Use `verify/1` for a checked decode.
  """
  @spec decode(binary()) :: {:ok, claims(), map()} | {:error, term()}
  def decode(token) when is_binary(token) do
    with [h, c, _s] <- String.split(String.trim(token), "."),
         {:ok, hjson} <- Base.url_decode64(h, padding: false),
         {:ok, cjson} <- Base.url_decode64(c, padding: false),
         {:ok, header} <- Jason.decode(hjson),
         {:ok, claims} <- Jason.decode(cjson) do
      {:ok, normalize(claims), header}
    else
      _ -> {:error, :malformed_jwt}
    end
  end

  @doc """
  Decode and verify a JWT's signature against its own `iss` claim, and check
  that it has not expired / is not yet valid. Returns `{:ok, claims}`.

  Note: this proves the token was signed by the key named in `iss`; callers
  must separately decide whether that issuer is trusted.
  """
  @spec verify(binary(), keyword()) :: {:ok, claims()} | {:error, term()}
  def verify(token, opts \\ []) do
    now = Keyword.get(opts, :now, System.os_time(:second))

    with [h, c, s] <- String.split(String.trim(token), "."),
         {:ok, claims, header} <- decode(token),
         :ok <- check_alg(header),
         {:ok, sig} <- Base.url_decode64(s, padding: false),
         iss when is_binary(iss) <- Map.get(claims, "iss", {:error, :missing_iss}),
         true <- NKeys.verify(iss, h <> "." <> c, sig) || {:error, :bad_signature},
         :ok <- check_times(claims, now) do
      {:ok, claims}
    else
      {:error, _} = e -> e
      _ -> {:error, :malformed_jwt}
    end
  end

  defp check_alg(%{"alg" => alg}) when alg in ["ed25519-nkey", "ed25519"], do: :ok
  defp check_alg(_), do: {:error, :unsupported_alg}

  defp check_times(claims, now) do
    exp = claims["exp"]
    nbf = claims["nbf"]

    cond do
      is_integer(exp) and exp != 0 and exp <= now -> {:error, :expired}
      is_integer(nbf) and nbf > now -> {:error, :not_yet_valid}
      true -> :ok
    end
  end

  # Fold v1 claims (type/fields at the top level) into the v2 `nats` shape.
  defp normalize(%{"nats" => nats} = claims) when is_map(nats), do: claims

  defp normalize(claims) do
    top = ~w(iss sub aud exp nbf iat jti name)
    {std, rest} = Map.split(claims, top)
    nats = rest |> Map.put_new("version", 1)
    Map.put(std, "nats", nats)
  end

  @doc "The `nats.type` of decoded claims (\"user\", \"account\", \"operator\", ...)."
  @spec type(claims()) :: binary() | nil
  def type(%{"nats" => %{"type" => t}}), do: t
  def type(_), do: nil

  @doc "Encode and sign claims with an encoded NKey seed. Useful for tests and tooling."
  @spec sign(map(), binary()) :: {:ok, binary()} | {:error, term()}
  def sign(claims, seed) do
    with {:ok, iss} <- NKeys.public_from_seed(seed) do
      claims =
        claims
        |> Map.put("iss", iss)
        |> Map.put_new("iat", System.os_time(:second))

      claims = Map.put_new_lazy(claims, "jti", fn -> jti(claims) end)

      header =
        Base.url_encode64(Jason.encode!(%{"typ" => "JWT", "alg" => "ed25519-nkey"}),
          padding: false
        )

      body = Base.url_encode64(Jason.encode!(claims), padding: false)
      {:ok, sig} = NKeys.sign(seed, header <> "." <> body)
      {:ok, header <> "." <> body <> "." <> Base.url_encode64(sig, padding: false)}
    end
  end

  defp jti(claims) do
    :crypto.hash(:sha256, Jason.encode!(Map.delete(claims, "jti")))
    |> Base.encode32(padding: false)
  end

  @doc """
  Extract the JWT and seed from a `.creds` file body (the `-----BEGIN NATS
  USER JWT-----` / `-----BEGIN USER NKEY SEED-----` format written by nsc).
  """
  @spec parse_creds(binary()) :: {:ok, jwt :: binary(), seed :: binary()} | {:error, term()}
  def parse_creds(body) do
    jwt =
      Regex.run(
        ~r/-----BEGIN NATS USER JWT-----\s*(\S+)\s*------END NATS USER JWT------/,
        body
      )

    seed =
      Regex.run(
        ~r/-----BEGIN USER NKEY SEED-----\s*(\S+)\s*------END USER NKEY SEED------/,
        body
      )

    case {jwt, seed} do
      {[_, j], [_, s]} -> {:ok, j, s}
      _ -> {:error, :invalid_creds}
    end
  end
end
