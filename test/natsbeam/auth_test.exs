defmodule Natsbeam.AuthTest do
  use ExUnit.Case, async: false
  import Natsbeam.TestHelpers

  setup do
    reset_config!()
    {:ok, port: start_listener!()}
  end

  defp try_connect(port, opts) do
    {sock, info} = connect(port)
    opts = if is_function(opts), do: opts.(info), else: opts
    send!(sock, "CONNECT " <> Jason.encode!(opts) <> "\r\nPING\r\n")
    {sock, recv(sock)}
  end

  test "token auth, plain and bcrypt", %{port: port} do
    Natsbeam.set_token("t0k3n")
    {_, info} = connect(port)
    assert info["auth_required"] == true
    assert {_, "PONG"} = try_connect(port, %{"auth_token" => "t0k3n"})
    assert {_, "-ERR 'Authorization Violation'"} = try_connect(port, %{"auth_token" => "nope"})
    assert {_, "-ERR 'Authorization Violation'"} = try_connect(port, %{})

    Natsbeam.set_token(Natsbeam.hash_password("hashed"))
    assert {_, "PONG"} = try_connect(port, %{"auth_token" => "hashed"})
  end

  test "ops before CONNECT are refused when auth is required", %{port: port} do
    Natsbeam.set_token("x")
    {sock, _} = connect(port)
    send!(sock, "SUB foo 1\r\n")
    assert "-ERR 'Authorization Violation'" = recv(sock)
  end

  test "password users, permissions and accounts", %{port: port} do
    Natsbeam.add_user("alice",
      password: "pw",
      account: "A",
      permissions: %{publish: ["a.>"], subscribe: ["a.>"]}
    )

    Natsbeam.add_user("bob", password: Natsbeam.hash_password("pw2"), account: "B")

    {a, "PONG"} = try_connect(port, %{"user" => "alice", "pass" => "pw"})
    {b, "PONG"} = try_connect(port, %{"user" => "bob", "pass" => "pw2"})

    assert {_, "-ERR 'Authorization Violation'"} =
             try_connect(port, %{"user" => "alice", "pass" => "bad"})

    assert {_, "-ERR 'Authorization Violation'"} =
             try_connect(port, %{"user" => "carol", "pass" => "pw"})

    send!(a, "PUB nope 1\r\nx\r\n")
    assert ~s(-ERR 'Permissions Violation for Publish to "nope"') = recv(a)
    send!(a, "SUB nope q 1\r\n")
    assert ~s(-ERR 'Permissions Violation for Subscription to "nope" using queue "q"') = recv(a)

    # accounts isolate traffic
    send!(b, "SUB a.> 1\r\nPING\r\n")
    assert "PONG" = recv(b)
    send!(a, "SUB a.> 1\r\nPUB a.x 1\r\ny\r\n")
    assert %{payload: "y"} = recv_msg(a)
    assert {:error, :timeout} = recv(b, 200)

    # removing a user kicks it
    Natsbeam.remove_user("alice")
    assert "-ERR 'Authorization Violation'" = recv(a)
    assert {:error, :closed} = recv(a)

    assert {_, "-ERR 'Authorization Violation'"} =
             try_connect(port, %{"user" => "alice", "pass" => "pw"})
  end

  test "nkey challenge/response", %{port: port} do
    {seed, pub} = Natsbeam.NKeys.generate(:user)
    {other_seed, _} = Natsbeam.NKeys.generate(:user)
    Natsbeam.add_nkey(pub, account: "NK")

    {_, info} = connect(port)
    assert is_binary(info["nonce"])

    assert {_, "PONG"} =
             try_connect(port, fn i -> %{"nkey" => pub, "sig" => sign_nonce(seed, i["nonce"])} end)

    assert {_, "-ERR 'Authorization Violation'"} =
             try_connect(port, fn i ->
               %{"nkey" => pub, "sig" => sign_nonce(other_seed, i["nonce"])}
             end)

    assert {_, "-ERR 'Authorization Violation'"} =
             try_connect(port, fn _ -> %{"nkey" => pub, "sig" => sign_nonce(seed, "wrong")} end)
  end

  test "operator mode with user JWTs", %{port: port} do
    f =
      operator_fixture(user: %{"pub" => %{"allow" => ["ok.>"]}, "sub" => %{"allow" => ["ok.>"]}})

    {:ok, _} = Natsbeam.add_operator(f.op_jwt)
    {:ok, _} = Natsbeam.add_account_jwt(f.acc_jwt)
    assert Natsbeam.info().operator_mode

    connect_jwt = fn i -> %{"jwt" => f.usr_jwt, "sig" => sign_nonce(f.usr_seed, i["nonce"])} end
    {sock, "PONG"} = try_connect(port, connect_jwt)
    send!(sock, "PUB no.way 1\r\nx\r\n")
    assert ~s(-ERR 'Permissions Violation for Publish to "no.way"') = recv(sock)
    assert [%{account: acc, kind: :jwt}] = Enum.filter(Natsbeam.clients(), &(&1.kind == :jwt))
    assert acc == f.acc_pub

    # wrong signer
    {bad_seed, _} = Natsbeam.NKeys.generate(:user)

    assert {_, "-ERR 'Authorization Violation'"} =
             try_connect(port, fn i ->
               %{"jwt" => f.usr_jwt, "sig" => sign_nonce(bad_seed, i["nonce"])}
             end)

    # account from an untrusted operator is rejected on load
    other = operator_fixture()
    assert {:error, :untrusted_account_issuer} = Natsbeam.add_account_jwt(other.acc_jwt)

    # user from an unknown account
    assert {_, "-ERR 'Authorization Violation'"} =
             try_connect(port, fn i ->
               %{"jwt" => other.usr_jwt, "sig" => sign_nonce(other.usr_seed, i["nonce"])}
             end)

    # removing the account JWT disconnects its clients
    Natsbeam.remove_account_jwt(f.acc_pub)
    assert "-ERR 'Account Removed'" = recv(sock)
  end

  test "bearer tokens, expiry and revocation", %{port: port} do
    f = operator_fixture(user: %{"bearer_token" => true})
    {:ok, _} = Natsbeam.add_operator(f.op_jwt)
    {:ok, _} = Natsbeam.add_account_jwt(f.acc_jwt)
    {sock, "PONG"} = try_connect(port, %{"jwt" => f.usr_jwt})

    # revoke the user via an updated account JWT
    {:ok, revoked_acc} =
      Natsbeam.JWT.sign(
        %{
          "sub" => f.acc_pub,
          "nats" => %{
            "type" => "account",
            "revocations" => %{f.usr_pub => System.os_time(:second) + 10}
          }
        },
        f.op_seed
      )

    {:ok, _} = Natsbeam.add_account_jwt(revoked_acc)
    assert "-ERR 'Authorization Violation'" = recv(sock)
    assert {_, "-ERR 'Authorization Violation'"} = try_connect(port, %{"jwt" => f.usr_jwt})

    # short-lived user gets expired
    f2 = operator_fixture(user: %{"bearer_token" => true}, exp: System.os_time(:second) + 1)
    {:ok, _} = Natsbeam.add_operator(f2.op_jwt)
    {:ok, _} = Natsbeam.add_account_jwt(f2.acc_jwt)
    {sock2, "PONG"} = try_connect(port, %{"jwt" => f2.usr_jwt})
    assert "-ERR 'User Authentication Expired'" = recv(sock2, 3_000)
  end

  test "scoped signing keys apply the template permissions", %{port: port} do
    f = operator_fixture()
    {sk_seed, sk_pub} = Natsbeam.NKeys.generate(:account)

    {:ok, acc_jwt} =
      Natsbeam.JWT.sign(
        %{
          "sub" => f.acc_pub,
          "nats" => %{
            "type" => "account",
            "signing_keys" => [
              %{
                "kind" => "user_scope",
                "key" => sk_pub,
                "template" => %{
                  "pub" => %{"allow" => ["tpl.>"]},
                  "sub" => %{"allow" => ["tpl.>"]}
                }
              }
            ]
          }
        },
        f.op_seed
      )

    {:ok, user_jwt} =
      Natsbeam.JWT.sign(
        %{"sub" => f.usr_pub, "nats" => %{"type" => "user", "issuer_account" => f.acc_pub}},
        sk_seed
      )

    {:ok, _} = Natsbeam.add_operator(f.op_jwt)
    {:ok, _} = Natsbeam.add_account_jwt(acc_jwt)

    {sock, "PONG"} =
      try_connect(port, fn i ->
        %{"jwt" => user_jwt, "sig" => sign_nonce(f.usr_seed, i["nonce"])}
      end)

    send!(sock, "PUB other 1\r\nx\r\n")
    assert ~s(-ERR 'Permissions Violation for Publish to "other"') = recv(sock)
    send!(sock, "PUB tpl.x 1\r\nx\r\nPING\r\n")
    assert "PONG" = recv(sock)
  end

  test "account connection limits from JWT", %{port: port} do
    f = operator_fixture(account: %{"limits" => %{"conn" => 1}}, user: %{"bearer_token" => true})
    {:ok, _} = Natsbeam.add_operator(f.op_jwt)
    {:ok, _} = Natsbeam.add_account_jwt(f.acc_jwt)
    assert {_, "PONG"} = try_connect(port, %{"jwt" => f.usr_jwt})

    assert {_, "-ERR 'maximum account active connections exceeded'"} =
             try_connect(port, %{"jwt" => f.usr_jwt})
  end

  defmodule Provider do
    @behaviour Natsbeam.Auth.Provider
    def authenticate(%{"user" => "magic"}, _nonce, client, opts) do
      {:ok,
       %Natsbeam.Auth.Identity{
         account: opts[:account],
         user: "magic",
         kind: :custom,
         tags: [client[:ip]]
       }}
    end

    def authenticate(_, _, _, _), do: :next
  end

  test "custom auth provider", %{port: port} do
    Natsbeam.set_auth_provider(Provider, account: "CUSTOM")
    assert {_, "PONG"} = try_connect(port, %{"user" => "magic"})
    assert {_, "-ERR 'Authorization Violation'"} = try_connect(port, %{"user" => "mundane"})
    assert wait_until(fn -> Enum.any?(Natsbeam.clients(), &(&1.account == "CUSTOM")) end)
    Natsbeam.set_auth_provider(nil)
    assert wait_until(fn -> not Enum.any?(Natsbeam.clients(), &(&1.account == "CUSTOM")) end)
  end

  test "auth timeout closes silent clients", %{port: port} do
    Natsbeam.Config.put(:auth_timeout, 100)
    Natsbeam.set_token("x")
    {sock, _} = connect(port)
    assert "-ERR 'Authentication Timeout'" = recv(sock, 1_000)
  end
end
