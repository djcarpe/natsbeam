defmodule Natsbeam.ParserTest do
  use ExUnit.Case, async: true
  alias Natsbeam.Protocol.Parser

  defp parse_all(chunks) do
    Enum.reduce(chunks, {[], Parser.new()}, fn chunk, {acc, p} ->
      {:ok, ops, p} = Parser.parse(p, chunk)
      {acc ++ ops, p}
    end)
    |> elem(0)
  end

  test "parses control ops" do
    assert [
             {:connect, %{"verbose" => true}},
             :ping,
             :pong,
             {:sub, "foo", nil, "1"},
             {:sub, "foo", "q", "2"},
             {:unsub, "1", nil},
             {:unsub, "2", 5}
           ] =
             parse_all([
               "CONNECT {\"verbose\":true}\r\nPING\r\npong\r\nSUB foo 1\r\nSUB foo q 2\r\nUNSUB 1\r\nUNSUB 2 5\r\n"
             ])
  end

  test "parses PUB split across arbitrary chunk boundaries" do
    frame = "PUB foo bar 10\r\nhello\r\nwor\r\nPING\r\n"

    for n <- 1..(byte_size(frame) - 1) do
      <<a::binary-size(n), b::binary>> = frame
      assert [{:pub, "foo", "bar", "hello\r\nwor"}, :ping] = parse_all([a, b])
    end
  end

  test "parses HPUB" do
    hdr = "NATS/1.0\r\nX-A: 1\r\n\r\n"
    frame = "HPUB foo #{byte_size(hdr)} #{byte_size(hdr) + 3}\r\n#{hdr}abc\r\n"
    assert [{:hpub, "foo", nil, ^hdr, "abc"}] = parse_all([frame])
  end

  test "rejects bad payload framing and oversize payloads" do
    assert {:error, "Parser Error", _} = Parser.parse(Parser.new(), "PUB foo 3\r\nabcde\r\n")

    assert {:error, "Maximum Payload Violation", _} =
             Parser.parse(Parser.new(max_payload: 2), "PUB foo 3\r\n")

    assert {:error, "Unknown Protocol Operation", _} = Parser.parse(Parser.new(), "BOGUS\r\n")

    assert {:error, "Maximum Control Line Exceeded", _} =
             Parser.parse(Parser.new(max_control_line: 8), "SUB aaaaaaaaaaaaaaaaaaaaa 1\r\n")
  end

  test "route frames with and without the account (pinned-account routes)" do
    frames =
      "RS+ ACC foo.> q 2\r\nRS- ACC foo.>\r\nRMSG ACC foo.bar + _INBOX.1 q 3\r\nabc\r\n" <>
        "HMSG ACC foo.bar 12 14\r\nNATS/1.0\r\n\r\nhi\r\n"

    assert {:ok, ops, _} = Parser.parse(Parser.new(mode: :route), frames)

    assert [
             {:rs_plus, "ACC", "foo.>", "q", 2},
             {:rs_minus, "ACC", "foo.>", nil},
             {:rmsg, "ACC", "foo.bar", "_INBOX.1", ["q"], nil, "abc"},
             {:rmsg, "ACC", "foo.bar", nil, [], "NATS/1.0\r\n\r\n", "hi"}
           ] = ops

    pinned =
      "RS+ foo.> q 2\r\nRS- foo.> q\r\nRS+ bar\r\nRMSG foo.bar | q 3\r\nabc\r\n" <>
        "RMSG foo.bar _INBOX.2 1\r\nx\r\nHMSG foo.bar _INBOX.3 12 14\r\nNATS/1.0\r\n\r\nhi\r\n"

    p = Parser.new(mode: :route, route_account: "$SYS")
    assert {:ok, ops, _} = Parser.parse(p, pinned)

    assert [
             {:rs_plus, "$SYS", "foo.>", "q", 2},
             {:rs_minus, "$SYS", "foo.>", "q"},
             {:rs_plus, "$SYS", "bar", nil, 1},
             {:rmsg, "$SYS", "foo.bar", nil, ["q"], nil, "abc"},
             {:rmsg, "$SYS", "foo.bar", "_INBOX.2", [], nil, "x"},
             {:rmsg, "$SYS", "foo.bar", "_INBOX.3", [], "NATS/1.0\r\n\r\n", "hi"}
           ] = ops
  end

  test "route parsing pauses after INFO so the pinned account can be set" do
    data = ~s(INFO {"route_account":"$SYS"}\r\nRS+ foo\r\n)

    assert {:ok, [{:info, %{"route_account" => "$SYS"}}], p} =
             Parser.parse(Parser.new(mode: :route), data)

    p = Parser.set_route_account(p, "$SYS")
    assert {:ok, [{:rs_plus, "$SYS", "foo", nil, 1}], _} = Parser.parse(p, "")
  end

  # --- fast-path coverage (client mode PUB/HPUB are pattern-matched directly)

  test "lowercase verbs still parse" do
    assert [{:pub, "foo", nil, "x"}, :ping, :pong] =
             parse_all(["pub foo 1\r\nx\r\nping\r\nPong\r\n"])
  end

  test "repeated spaces and tabs between arguments still parse" do
    assert [{:pub, "foo", nil, "x"}, {:pub, "foo", "r", "y"}] =
             parse_all(["PUB  foo\t1\r\nx\r\nPUB foo r  1 \r\ny\r\n"])
  end

  test "LF-terminated control lines still parse" do
    # (Only when no CRLF is in the same chunk: the line scanner looks for
    # CRLF first, as the legacy parser did.)
    assert [:ping, {:sub, "foo", nil, "1"}] = parse_all(["PING\n", "SUB foo 1\n"])
    assert [{:pub, "foo", nil, "x"}] = parse_all(["PUB foo 1\n", "x\r\n"])
  end

  test "sizes with a sign or trailing garbage" do
    # Integer.parse accepts a leading '+' (legacy behaviour, kept).
    assert [{:pub, "foo", nil, "x"}] = parse_all(["PUB foo +1\r\nx\r\n"])
    assert {:error, "Parser Error", _} = Parser.parse(Parser.new(), "PUB foo 1a\r\nx\r\n")
    assert {:error, "Parser Error", _} = Parser.parse(Parser.new(), "PUB foo -1\r\nx\r\n")
    assert {:error, "Parser Error", _} = Parser.parse(Parser.new(), "HPUB foo 1x 20\r\n")
  end

  test "payload framing errors on the fast path" do
    assert {:error, "Parser Error", %{buf: ""}} =
             Parser.parse(Parser.new(), "PUB foo 3\r\nabcde\r\n")

    assert {:error, "Parser Error", _} =
             Parser.parse(Parser.new(), "HPUB foo 12 13\r\nNATS/1.0\r\n\r\nxXX")

    # HPUB with a bad header block: error after the payload is consumed.
    assert {:error, "Parser Error", %{buf: "PING\r\n"}} =
             Parser.parse(Parser.new(), "HPUB foo 12 13\r\nHTTP/1.0\r\n\r\nx\r\nPING\r\n")

    # hdr_len > total_len, or shorter than the minimal header block.
    assert {:error, "Parser Error", _} = Parser.parse(Parser.new(), "HPUB foo 14 13\r\n")
    assert {:error, "Parser Error", _} = Parser.parse(Parser.new(), "HPUB foo 3 13\r\n")
  end

  test "limits on the fast path" do
    assert {:error, "Maximum Control Line Exceeded", _} =
             Parser.parse(Parser.new(max_control_line: 8), "PUB aaaaaaaaaa 1\r\nx\r\n")

    # A partial line longer than the limit is rejected before its CRLF arrives.
    assert {:error, "Maximum Control Line Exceeded", _} =
             Parser.parse(Parser.new(max_control_line: 8), "PUB aaaaaaaaaaaaaaaa")

    assert {:error, "Maximum Payload Violation", %{buf: "x\r\n"}} =
             Parser.parse(Parser.new(max_payload: 2), "PUB foo 3\r\nx\r\n")

    assert {:error, "Maximum Payload Violation", _} =
             Parser.parse(Parser.new(max_payload: 12), "HPUB foo 12 13\r\n")
  end

  test "parses HPUB split across arbitrary chunk boundaries" do
    hdr = "NATS/1.0\r\nX-A: 1\r\n\r\n"
    frame = "HPUB foo bar #{byte_size(hdr)} #{byte_size(hdr) + 5}\r\n#{hdr}ab\r\nc\r\nPONG\r\n"

    for n <- 1..(byte_size(frame) - 1) do
      <<a::binary-size(n), b::binary>> = frame
      assert [{:hpub, "foo", "bar", ^hdr, "ab\r\nc"}, :pong] = parse_all([a, b])
    end
  end

  test "many frames in one chunk, with leftovers carried over" do
    frames =
      for i <- 1..50, do: "PUB s.#{i} #{rem(i, 7)}\r\n#{String.duplicate("p", rem(i, 7))}\r\n"

    data = Enum.join(frames)
    {:ok, ops, p} = Parser.parse(Parser.new(), data <> "PUB tail")
    assert length(ops) == 50
    assert p.buf == "PUB tail"
    assert {:ok, [{:pub, "tail", nil, "zz"}], %{buf: ""}} = Parser.parse(p, " 2\r\nzz\r\n")
  end
end
