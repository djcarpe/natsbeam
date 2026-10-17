defmodule Natsbeam.JWTTest do
  use ExUnit.Case, async: true
  alias Natsbeam.{JWT, NKeys}

  test "signs and verifies" do
    {seed, pub} = NKeys.generate(:account)
    {:ok, jwt} = JWT.sign(%{"sub" => pub, "nats" => %{"type" => "account"}}, seed)
    assert {:ok, claims} = JWT.verify(jwt)
    assert claims["iss"] == pub
    assert JWT.type(claims) == "account"
    assert is_binary(claims["jti"])
  end

  test "detects tampering and expiry" do
    {seed, pub} = NKeys.generate(:user)
    {:ok, jwt} = JWT.sign(%{"sub" => pub, "nats" => %{"type" => "user"}, "exp" => 1}, seed)
    assert {:error, :expired} = JWT.verify(jwt)
    assert {:ok, _} = JWT.verify(jwt, now: 0)

    [h, c, s] = String.split(jwt, ".")
    {:ok, body} = Base.url_decode64(c, padding: false)
    tampered = Base.url_encode64(String.replace(body, "user", "USER"), padding: false)
    assert {:error, :bad_signature} = JWT.verify(Enum.join([h, tampered, s], "."), now: 0)
  end

  test "parses creds files" do
    body = """
    -----BEGIN NATS USER JWT-----
    eyJ.abc.def
    ------END NATS USER JWT------

    -----BEGIN USER NKEY SEED-----
    SUABC
    ------END USER NKEY SEED------
    """

    assert {:ok, "eyJ.abc.def", "SUABC"} = JWT.parse_creds(body)
  end
end
