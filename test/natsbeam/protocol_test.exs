defmodule Natsbeam.ProtocolTest do
  use ExUnit.Case, async: false
  import Natsbeam.TestHelpers

  setup do
    reset_config!()
    {:ok, port: start_listener!()}
  end

  test "INFO announces the server", %{port: port} do
    {_sock, info} = connect(port)
    assert String.starts_with?(info["server_id"], "N")
    assert info["headers"] == true
    assert info["max_payload"] == 1_048_576
    refute Map.has_key?(info, "auth_required")
  end

  test "verbose mode acknowledges and PING gets PONG", %{port: port} do
    {sock, _} = connect(port)
    send!(sock, ~s(CONNECT {"verbose":true}\r\nPING\r\nSUB foo 1\r\n))
    assert "+OK" = recv(sock)
    assert "PONG" = recv(sock)
    assert "+OK" = recv(sock)
  end

  test "pub/sub with reply, headers and unsub max", %{port: port} do
    {a, _} = connect!(port, %{"headers" => true})
    {b, _} = connect!(port)

    send!(a, "SUB foo.* 7\r\nPING\r\n")
    assert "PONG" = recv(a)
    send!(b, "PUB foo.bar reply.me 5\r\nhello\r\n")

    assert %{op: :msg, subject: "foo.bar", sid: "7", reply: "reply.me", payload: "hello"} =
             recv_msg(a)

    hdr = "NATS/1.0\r\nX-Trace: 1\r\n\r\n"
    send!(b, "HPUB foo.baz #{byte_size(hdr)} #{byte_size(hdr) + 2}\r\n#{hdr}hi\r\n")
    assert %{op: :hmsg, subject: "foo.baz", headers: ^hdr, payload: "hi"} = recv_msg(a)

    # b has no header support: headers are stripped
    send!(b, "SUB foo.> 1\r\nPING\r\n")
    assert "PONG" = recv(b)
    send!(a, "HPUB foo.x #{byte_size(hdr)} #{byte_size(hdr) + 2}\r\n#{hdr}yo\r\n")
    assert %{op: :msg, payload: "yo", headers: nil} = recv_msg(b)
    # a hears its own publish (echo on by default)
    assert %{op: :hmsg, payload: "yo"} = recv_msg(a)

    # UNSUB with max: the limit counts all messages ever received on the sid (3 so far)
    send!(a, "UNSUB 7 4\r\n")
    send!(b, "PUB foo.1 1\r\na\r\nPUB foo.2 1\r\nb\r\nPING\r\n")
    assert %{payload: "a"} = recv_msg(a)
    assert {:error, :timeout} = recv(a, 200)
  end

  test "echo can be disabled", %{port: port} do
    {a, _} = connect!(port, %{"echo" => false})
    send!(a, "SUB x 1\r\nPUB x 1\r\na\r\nPING\r\n")
    assert "PONG" = recv(a)
  end

  test "a publisher's cached matches follow subscription changes", %{port: port} do
    {p, _} = connect!(port, %{"echo" => false})
    {a, _} = connect!(port)
    {b, _} = connect!(port)

    # First publish caches "no subscribers" for the subject.
    send!(p, "PUB cache.x 1\r\n0\r\nPING\r\n")
    assert "PONG" = recv(p)

    # A new subscription must invalidate the publisher's cache.
    send!(a, "SUB cache.x 1\r\nPING\r\n")
    assert "PONG" = recv(a)
    send!(p, "PUB cache.x 1\r\n1\r\n")
    assert %{sid: "1", payload: "1"} = recv_msg(a)

    # So must a wildcard subscription from another client...
    send!(b, "SUB cache.* 2\r\nPING\r\n")
    assert "PONG" = recv(b)
    send!(p, "PUB cache.x 1\r\n2\r\n")
    assert %{sid: "1", payload: "2"} = recv_msg(a)
    assert %{sid: "2", payload: "2"} = recv_msg(b)

    # ...an UNSUB...
    send!(a, "UNSUB 1\r\nPING\r\n")
    assert "PONG" = recv(a)
    send!(p, "PUB cache.x 1\r\n3\r\n")
    assert %{sid: "2", payload: "3"} = recv_msg(b)
    assert {:error, :timeout} = recv(a, 200)

    # ...and a subscriber going away entirely.
    :gen_tcp.close(b)
    wait_until(fn -> length(Natsbeam.clients()) == 2 end)
    send!(p, "PUB cache.x 1\r\n4\r\nPING\r\n")
    assert "PONG" = recv(p)
    assert {:error, :timeout} = recv(a, 200)
  end

  test "queue groups receive one copy per group", %{port: port} do
    socks = for _ <- 1..3, do: connect!(port) |> elem(0)
    for s <- socks, do: send!(s, "SUB q.> grp 1\r\nPING\r\n")
    for s <- socks, do: assert("PONG" = recv(s))
    {p, _} = connect!(port)
    for i <- 1..20, do: send!(p, "PUB q.x 1\r\n#{rem(i, 10)}\r\n")

    got =
      socks
      |> Enum.map(fn s ->
        Stream.repeatedly(fn -> recv_msg(s, 150) end) |> Enum.take_while(&is_map/1) |> length()
      end)
      |> Enum.sum()

    assert got == 20
  end

  test "no_responders yields a 503 to the requester", %{port: port} do
    {a, _} = connect!(port, %{"headers" => true, "no_responders" => true})
    send!(a, "SUB _INBOX.1 9\r\nPUB nobody _INBOX.1 0\r\n\r\n")

    assert %{op: :hmsg, subject: "_INBOX.1", headers: "NATS/1.0 503\r\n\r\n", payload: ""} =
             recv_msg(a)
  end

  test "protocol errors", %{port: port} do
    {a, _} = connect!(port)
    send!(a, "PUB bad..subject 1\r\nx\r\n")
    assert "-ERR 'Invalid Subject'" = recv(a)
    send!(a, "SUB foo.>.bar 1\r\n")
    assert "-ERR 'Invalid Subject'" = recv(a)
    send!(a, "PUB foo 2000000\r\n")
    assert "-ERR 'Maximum Payload Violation'" = recv(a)
    assert {:error, :closed} = recv(a)

    {b, _} = connect!(port)
    send!(b, "WHAT\r\n")
    assert "-ERR 'Unknown Protocol Operation'" = recv(b)
    assert {:error, :closed} = recv(b)
  end

  test "pedantic mode rejects wildcard publishes", %{port: port} do
    {a, _} = connect!(port, %{"pedantic" => true})
    send!(a, "PUB foo.* 1\r\nx\r\n")
    assert "-ERR 'Invalid Publish Subject'" = recv(a)
  end

  test "clients are listed and can be disconnected", %{port: port} do
    {a, _} = connect!(port, %{"name" => "tester"})
    assert wait_until(fn -> Enum.any?(Natsbeam.clients(), &(&1.name == "tester")) end)
    c = Enum.find(Natsbeam.clients(), &(&1.name == "tester"))
    assert c.account == "$G"
    :ok = Natsbeam.disconnect(c.id, "bye")
    assert "-ERR 'bye'" = recv(a)
    assert {:error, :closed} = recv(a)
  end

  test "server pings idle clients and drops stale ones", %{port: port} do
    Natsbeam.Config.update(&Map.merge(&1, %{ping_interval: 100, ping_max_outstanding: 1}))
    {a, _} = connect!(port)
    assert "PING" = recv(a)
    send!(a, "PONG\r\n")
    assert "PING" = recv(a)
    assert "-ERR 'Stale Connection'" = recv(a, 1_000)
  end
end
