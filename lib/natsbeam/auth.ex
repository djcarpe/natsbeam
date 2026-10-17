defmodule Natsbeam.Auth do
  @moduledoc """
  Authenticates `CONNECT` requests.

  Supported, in the order nats-server evaluates them:

    1. a custom `Natsbeam.Auth.Provider` (BEAM-native hook)
    2. operator mode: user JWT + NKey nonce signature (`jwt`/`sig`), accounts
       resolved from the in-memory account JWT store
    3. NKey users (`nkey`/`sig`)
    4. username/password users (`user`/`pass`, bcrypt or plaintext)
    5. shared token (`auth_token`, bcrypt or plaintext)
    6. no auth configured: anonymous into the default account

  All of the configuration is live and can be edited through `Natsbeam`.
  """
  require Logger
  alias Natsbeam.{Config, JWT, NKeys, Permissions}
  alias Natsbeam.Auth.Identity

  @type connect_opts :: map()

  @doc "Whether clients must authenticate (drives `auth_required` in INFO)."
  @spec required?(map()) :: boolean()
  def required?(auth \\ Config.get(:auth)) do
    auth.provider != nil or auth.token != nil or auth.users != %{} or auth.nkeys != %{} or
      auth.operators != %{}
  end

  @doc "Whether a nonce should be sent (NKey or JWT auth possible)."
  def nonce_required?(auth \\ Config.get(:auth)) do
    auth.provider != nil or auth.nkeys != %{} or auth.operators != %{}
  end

  @doc "Operator (decentralised JWT) mode?"
  def operator_mode?(auth \\ Config.get(:auth)), do: auth.operators != %{}

  @doc """
  Authenticate a CONNECT. `client` carries `:ip`, `:tls?`, `:id` etc.
  Returns `{:ok, identity}` or `{:error, reason}` where reason is a NATS
  error string suitable for `-ERR`.
  """
  @spec authenticate(connect_opts(), binary() | nil, map()) ::
          {:ok, Identity.t()} | {:error, binary()}
  def authenticate(opts, nonce, client) do
    auth = Config.get(:auth)

    result =
      with :next <- custom(auth, opts, nonce, client),
           :next <- operator(auth, opts, nonce, client),
           :next <- nkey(auth, opts, nonce),
           :next <- password(auth, opts),
           :next <- token(auth, opts) do
        if required?(auth) do
          {:error, "Authorization Violation"}
        else
          {:ok, %Identity{account: auth.default_account, kind: :none}}
        end
      end

    case result do
      {:ok, %Identity{} = id} ->
        {:ok, finalize(id, auth)}

      {:error, reason} when is_binary(reason) ->
        {:error, reason}

      {:error, reason} ->
        Logger.debug("natsbeam: auth failed: #{inspect(reason)}")
        {:error, "Authorization Violation"}
    end
  end

  defp finalize(%Identity{} = id, auth) do
    account = id.account || auth.default_account
    %{id | account: account}
  end

  # ------------------------------------------------------------------ custom

  defp custom(%{provider: nil}, _, _, _), do: :next

  defp custom(%{provider: {mod, popts}}, opts, nonce, client) do
    case mod.authenticate(opts, nonce, client, popts) do
      {:ok, %Identity{} = id} -> {:ok, %{id | kind: id.kind || :custom}}
      {:error, reason} -> {:error, reason}
      :next -> :next
    end
  end

  # ---------------------------------------------------------------- operator

  defp operator(%{operators: ops}, _opts, _nonce, _client) when ops == %{}, do: :next

  defp operator(auth, opts, nonce, client) do
    case Map.get(opts, "jwt") do
      jwt when is_binary(jwt) and jwt != "" ->
        with {:ok, user_claims} <- verify_user_jwt(jwt, auth),
             {:ok, account_key, account_claims, scope} <- resolve_account(user_claims, auth),
             :ok <- check_revoked(account_claims, user_claims),
             :ok <- check_nonce(user_claims, opts, nonce),
             :ok <- check_src(user_claims, client),
             :ok <- check_connection_type(user_claims, client) do
          {:ok, identity_from_jwt(user_claims, account_key, account_claims, scope)}
        else
          {:error, reason} -> {:error, {:jwt, reason}}
        end

      _ ->
        # In operator mode a JWT is mandatory; give the nats-server error text.
        {:error, "Authorization Violation"}
    end
  end

  defp verify_user_jwt(jwt, _auth) do
    with {:ok, claims} <- JWT.verify(jwt),
         "user" <- JWT.type(claims) || {:error, :not_user_jwt},
         true <- NKeys.valid_public?(claims["sub"], :user) || {:error, :bad_subject} do
      {:ok, claims}
    else
      {:error, _} = e -> e
      _ -> {:error, :not_user_jwt}
    end
  end

  # Accounts: `iss` is the account key or one of its signing keys; when a
  # signing key signs, `nats.issuer_account` names the account.
  defp resolve_account(user_claims, auth) do
    iss = user_claims["iss"]
    account_key = get_in(user_claims, ["nats", "issuer_account"]) || iss

    with {:ok, account_claims} <- fetch_account(account_key, auth),
         :ok <- check_account_trusted(account_claims, auth),
         :ok <- check_account_expiry(account_claims),
         {:ok, scope} <- check_issuer(account_claims, iss, account_key) do
      {:ok, account_key, account_claims, scope}
    end
  end

  defp fetch_account(key, auth) do
    case Map.get(auth.account_jwts, key) do
      nil -> {:error, {:unknown_account, key}}
      claims -> {:ok, claims}
    end
  end

  @doc "Is this verified account JWT issued by a trusted operator (or its signing key)?"
  def check_account_trusted(account_claims, auth) do
    iss = account_claims["iss"]

    trusted =
      Enum.any?(auth.operators, fn {op_key, op_claims} ->
        op_key == iss or iss in (get_in(op_claims, ["nats", "signing_keys"]) || [])
      end)

    if trusted, do: :ok, else: {:error, :untrusted_account_issuer}
  end

  defp check_account_expiry(%{"exp" => exp}) when is_integer(exp) and exp > 0 do
    if exp > System.os_time(:second), do: :ok, else: {:error, :account_expired}
  end

  defp check_account_expiry(_), do: :ok

  # Returns {:ok, nil} for a plain issuer or {:ok, template} for a scoped signing key.
  defp check_issuer(account_claims, iss, account_key) when iss == account_key do
    if account_claims["sub"] == account_key, do: {:ok, nil}, else: {:error, :issuer_mismatch}
  end

  defp check_issuer(account_claims, iss, _account_key) do
    keys = get_in(account_claims, ["nats", "signing_keys"]) || []

    Enum.find_value(keys, {:error, :issuer_not_signing_key}, fn
      ^iss -> {:ok, nil}
      %{"key" => ^iss, "kind" => "user_scope"} = scope -> {:ok, scope["template"] || %{}}
      %{"key" => ^iss} -> {:ok, nil}
      _ -> nil
    end)
  end

  defp check_revoked(account_claims, user_claims) do
    revs = get_in(account_claims, ["nats", "revocations"]) || %{}
    sub = user_claims["sub"]
    iat = user_claims["iat"] || 0

    revoked? =
      Enum.any?([sub, "*"], fn k ->
        case Map.get(revs, k) do
          ts when is_integer(ts) -> ts >= iat
          _ -> false
        end
      end)

    if revoked?, do: {:error, :revoked}, else: :ok
  end

  defp check_nonce(user_claims, opts, nonce) do
    if get_in(user_claims, ["nats", "bearer_token"]) == true do
      :ok
    else
      verify_sig(user_claims["sub"], opts["sig"], nonce)
    end
  end

  defp verify_sig(_pub, nil, _nonce), do: {:error, :missing_sig}
  defp verify_sig(_pub, _sig, nil), do: {:error, :no_nonce}

  defp verify_sig(pub, sig, nonce) do
    with {:ok, raw} <- decode_sig(sig),
         true <- NKeys.verify(pub, nonce, raw) do
      :ok
    else
      _ -> {:error, :bad_signature}
    end
  end

  defp decode_sig(sig) do
    with :error <- Base.url_decode64(sig, padding: false),
         :error <- Base.url_decode64(sig),
         :error <- Base.decode64(sig, padding: false) do
      Base.decode64(sig)
    end
  end

  defp check_src(user_claims, client) do
    case get_in(user_claims, ["nats", "src"]) do
      nil ->
        :ok

      [] ->
        :ok

      cidrs when is_list(cidrs) ->
        if Enum.any?(cidrs, &ip_in_cidr?(client[:ip], &1)),
          do: :ok,
          else: {:error, :src_not_allowed}

      cidr when is_binary(cidr) ->
        check_src(put_in(user_claims, ["nats", "src"], String.split(cidr, ",")), client)
    end
  end

  defp check_connection_type(user_claims, client) do
    case get_in(user_claims, ["nats", "allowed_connection_types"]) do
      nil ->
        :ok

      [] ->
        :ok

      types ->
        if (client[:connection_type] || "STANDARD") in types,
          do: :ok,
          else: {:error, :connection_type}
    end
  end

  defp identity_from_jwt(user_claims, account_key, account_claims, scope) do
    nats = user_claims["nats"] || %{}
    source = scope || nats

    own_perms? =
      Map.has_key?(source, "pub") or Map.has_key?(source, "sub") or Map.has_key?(source, "resp")

    perms =
      if own_perms? do
        Permissions.from_map(source)
      else
        Permissions.from_map(get_in(account_claims, ["nats", "default_permissions"]))
      end

    account_limits = get_in(account_claims, ["nats", "limits"]) || %{}

    %Identity{
      account: account_key,
      user: user_claims["name"] || user_claims["sub"],
      kind: :jwt,
      nkey: user_claims["sub"],
      permissions: perms,
      max_payload: min_limit(source["payload"], account_limits["payload"]),
      max_subscriptions: min_limit(source["subs"], account_limits["subs"]),
      expires_at: user_claims["exp"],
      claims: user_claims,
      tags: nats["tags"] || []
    }
  end

  defp min_limit(a, b) do
    [a, b]
    |> Enum.filter(&(is_integer(&1) and &1 >= 0))
    |> case do
      [] -> nil
      xs -> Enum.min(xs)
    end
  end

  # -------------------------------------------------------------------- nkey

  defp nkey(%{nkeys: nkeys}, _opts, _nonce) when nkeys == %{}, do: :next

  defp nkey(%{nkeys: nkeys}, %{"nkey" => pub} = opts, nonce) when is_binary(pub) do
    case Map.get(nkeys, pub) do
      nil ->
        {:error, :unknown_nkey}

      user ->
        case verify_sig(pub, opts["sig"], nonce) do
          :ok -> {:ok, identity_from_user(user, pub, :nkey, pub)}
          err -> err
        end
    end
  end

  defp nkey(_, _, _), do: :next

  # ---------------------------------------------------------------- password

  defp password(%{users: users}, _opts) when users == %{}, do: :next

  defp password(%{users: users}, %{"user" => name} = opts) when is_binary(name) do
    pass = Map.get(opts, "pass") || ""

    case Map.get(users, name) do
      nil ->
        # Burn comparable time so user enumeration isn't trivial.
        secret_match?("$2b$10$000000000000000000000uGUUqK5bYgIBrB4iT1RDK2OnQ3fWp5xq", pass)
        {:error, :unknown_user}

      %{password: expected} = user ->
        if secret_match?(expected, pass),
          do: {:ok, identity_from_user(user, name, :password, nil)},
          else: {:error, :bad_password}
    end
  end

  defp password(_, _), do: :next

  # ------------------------------------------------------------------- token

  defp token(%{token: nil}, _opts), do: :next

  defp token(%{token: expected, default_account: default}, opts) do
    provided = Map.get(opts, "auth_token") || Map.get(opts, "pass") || ""

    if secret_match?(expected, provided),
      do: {:ok, %Identity{account: default, kind: :token}},
      else: {:error, :bad_token}
  end

  # ----------------------------------------------------------------- helpers

  @doc false
  def identity_from_user(user, name, kind, nkey) do
    %Identity{
      account: user[:account],
      user: user[:user] || name,
      kind: kind,
      nkey: nkey,
      permissions: user[:permissions],
      max_payload: user[:max_payload],
      max_subscriptions: user[:max_subscriptions],
      expires_at: user[:expires_at]
    }
  end

  @doc "Normalise a user spec map from config (atom or string keys, loose permissions)."
  def normalize_user(user) do
    user = Map.new(user, fn {k, v} -> {to_atom(k), v} end)

    user
    |> Map.update(:permissions, nil, &Permissions.from_map/1)
    |> Map.update(:account, nil, &(&1 && to_string(&1)))
  end

  defp to_atom(k) when is_atom(k), do: k
  defp to_atom(k) when is_binary(k), do: String.to_atom(k)

  @doc "Constant-time secret comparison; `expected` may be a bcrypt hash."
  @spec secret_match?(binary() | nil, binary()) :: boolean()
  def secret_match?(nil, _), do: false

  def secret_match?(<<"$2", _::binary>> = hash, provided) do
    Bcrypt.verify_pass(provided, hash)
  rescue
    _ -> false
  end

  def secret_match?(expected, provided) when is_binary(expected) and is_binary(provided) do
    byte_size(expected) == byte_size(provided) and :crypto.hash_equals(expected, provided)
  end

  def secret_match?(_, _), do: false

  @doc false
  def ip_in_cidr?(nil, _), do: false

  def ip_in_cidr?(ip, cidr) when is_binary(ip) do
    case :inet.parse_address(String.to_charlist(ip)) do
      {:ok, addr} -> ip_in_cidr?(addr, cidr)
      _ -> false
    end
  end

  def ip_in_cidr?(ip, cidr) when is_tuple(ip) and is_binary(cidr) do
    {net, bits} =
      case String.split(String.trim(cidr), "/") do
        [n] -> {n, nil}
        [n, b] -> {n, String.to_integer(b)}
      end

    case :inet.parse_address(String.to_charlist(net)) do
      {:ok, naddr} when tuple_size(naddr) == tuple_size(ip) ->
        total = tuple_size(ip) * if(tuple_size(ip) == 4, do: 8, else: 16)
        bits = bits || total
        ip_int = ip_to_int(ip, total)
        net_int = ip_to_int(naddr, total)
        shift = total - bits
        Bitwise.bsr(ip_int, shift) == Bitwise.bsr(net_int, shift)

      _ ->
        false
    end
  end

  defp ip_to_int(ip, total) do
    width = div(total, tuple_size(ip))
    Enum.reduce(Tuple.to_list(ip), 0, fn part, acc -> Bitwise.bsl(acc, width) + part end)
  end
end
