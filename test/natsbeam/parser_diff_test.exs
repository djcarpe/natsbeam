defmodule Natsbeam.ParserDiffTest do
  @moduledoc """
  Differential test: the rewritten parser must behave exactly like the
  parser it replaced (`test/support/legacy_parser.ex`) on random streams of
  valid and malformed frames, split at random byte boundaries.
  """
  use ExUnit.Case, async: true
  alias Natsbeam.Protocol.{LegacyParser, Parser}

  @seed {20_261_015, 4222, 1}
  @streams 3000

  test "client mode: new parser == legacy parser on random frame streams" do
    :rand.seed(:exsss, @seed)

    for i <- 1..@streams do
      opts = [max_control_line: pick([64, 4096, 4096]), max_payload: pick([16, 1_048_576])]
      stream = Enum.map_join(1..pick([1, 2, 3, 5, 8]), "", fn _ -> client_frame() end)
      compare(opts, stream, i)
    end
  end

  test "route and leaf modes: new parser == legacy parser on random frame streams" do
    :rand.seed(:exsss, @seed)

    for i <- 1..div(@streams, 3) do
      mode = pick([:route, :route, :leaf, :gateway])
      ra = if mode == :route, do: pick([nil, nil, "$SYS"])
      opts = [mode: mode, route_account: ra, max_payload: pick([16, 1_048_576])]
      stream = Enum.map_join(1..pick([1, 2, 3, 5]), "", fn _ -> link_frame(mode) end)
      compare(opts, stream, i)
    end
  end

  defp compare(opts, stream, i) do
    chunks = chunk(stream, pick([1, 1, 2, 3, 5, 9]))
    expected = run(LegacyParser, opts, chunks)
    actual = run(Parser, opts, chunks)

    if expected != actual do
      flunk("""
      parser divergence on stream ##{i} (seed #{inspect(@seed)}), opts #{inspect(opts)}
      stream:   #{inspect(stream)}
      chunks:   #{inspect(chunks)}
      legacy:   #{inspect(expected)}
      new:      #{inspect(actual)}
      """)
    end
  end

  # Feed both parsers chunk by chunk; stop at the first error (a client
  # connection is closed on error). The result also includes the leftover
  # buffer so partial-frame handling is compared too.
  defp run(mod, opts, chunks) do
    Enum.reduce_while(chunks, {[], mod.new(opts)}, fn chunk, {acc, p} ->
      case mod.parse(p, chunk) do
        {:ok, ops, p} -> {:cont, {acc ++ ops, p}}
        {:error, reason, p} -> {:halt, {acc ++ [{:error, reason}], p}}
      end
    end)
    |> then(fn {ops, p} -> {ops, p.buf, p.pending != nil} end)
  end

  defp chunk(bin, 1), do: [bin]

  defp chunk(bin, n) do
    size = byte_size(bin)

    cuts =
      Enum.map(1..(n - 1), fn _ -> :rand.uniform(size + 1) - 1 end)
      |> Enum.sort()
      |> Enum.uniq()

    {parts, last} =
      Enum.reduce(cuts, {[], 0}, fn cut, {parts, from} ->
        {[binary_part(bin, from, cut - from) | parts], cut}
      end)

    Enum.reverse([binary_part(bin, last, size - last) | parts])
  end

  # ------------------------------------------------------------ generators

  defp pick(list), do: Enum.at(list, :rand.uniform(length(list)) - 1)

  defp subject do
    toks = for _ <- 1..pick([1, 1, 2, 3]), do: pick(~w(foo bar baz.qux * > _INBOX.abc x 42 a.b))
    Enum.join(toks, ".")
  end

  defp payload do
    pick(["", "x", "hello", "hello\r\nworld", :binary.copy("p", 17), :binary.copy("z", 200)])
  end

  defp sep, do: pick([" ", " ", " ", " ", "  ", "\t", " \t"])
  defp eol, do: pick(["\r\n", "\r\n", "\r\n", "\r\n", "\n", " \r\n"])
  defp verb(v), do: pick([v, v, v, v, String.downcase(v), String.capitalize(v)])

  defp size_of(payload) do
    n = byte_size(payload)
    pick([n, n, n, n, n, n, n + 1, n - 1, "+#{n}", "#{n}a", "-1", "0#{n}"]) |> to_string()
  end

  defp headers do
    pick([
      "NATS/1.0\r\n\r\n",
      "NATS/1.0\r\nX-A: 1\r\n\r\n",
      "NATS/1.0\r\nX-A: 1\r\nX-B: two\r\n\r\n",
      "NATS/1.0\r\n",
      "HTTP/1.0\r\n\r\n",
      ""
    ])
  end

  defp client_frame do
    case :rand.uniform(14) do
      n when n <= 5 ->
        pl = payload()
        reply = pick([nil, nil, "_INBOX.1", "r"])
        args = Enum.reject([subject(), reply, size_of(pl)], &is_nil/1)

        verb("PUB") <>
          sep() <>
          Enum.join(args, sep()) <> eol() <> pl <> pick(["\r\n", "\r\n", "\r\n", "\n", "XX"])

      n when n <= 8 ->
        hdr = headers()
        pl = payload()
        reply = pick([nil, nil, "_INBOX.2"])
        h = byte_size(hdr)
        t = h + byte_size(pl)
        hs = pick([h, h, h, h - 1, h + 1, 3, "x"]) |> to_string()
        ts = pick([t, t, t, t - 1, t + 2, "y"]) |> to_string()
        args = Enum.reject([subject(), reply, hs, ts], &is_nil/1)

        verb("HPUB") <>
          sep() <> Enum.join(args, sep()) <> eol() <> hdr <> pl <> pick(["\r\n", "\r\n", "\n"])

      9 ->
        queue = pick([nil, nil, "q1"])
        args = Enum.reject([subject(), queue, "#{:rand.uniform(99)}"], &is_nil/1)
        verb("SUB") <> sep() <> Enum.join(args, sep()) <> eol()

      10 ->
        max = pick([nil, nil, "5", "x"])
        args = Enum.reject(["#{:rand.uniform(99)}", max], &is_nil/1)
        verb("UNSUB") <> sep() <> Enum.join(args, sep()) <> eol()

      11 ->
        verb(pick(["PING", "PONG"])) <> pick(["", "", "", " extra"]) <> eol()

      12 ->
        pick([
          ~s(CONNECT {"verbose":true,"name":"x"}),
          ~s(CONNECT {"verbose":true),
          ~s(INFO {"server_id":"abc"}),
          "+OK",
          "-ERR 'Unknown'",
          "BOGUS 1 2",
          "",
          :binary.copy("A", 70)
        ]) <> eol()

      _ ->
        # Stray bytes that are not a complete frame.
        pick([
          "PUB",
          "PU",
          "P",
          "PUB foo",
          "PUB foo 3",
          "PUB foo 3\r",
          "\r\n",
          "HPUB a 12",
          "pin"
        ])
    end
  end

  defp link_frame(mode) do
    acc = if mode == :leaf, do: [], else: ["ACC"]
    pl = payload()
    hdr = headers()
    h = byte_size(hdr)

    targets =
      pick([
        [],
        ["_INBOX.9"],
        ["+", "_INBOX.9", "q1"],
        ["+", "_INBOX.9", "q1", "q2"],
        ["|", "q1"],
        ["|", "q1", "q2"]
      ])

    frame =
      case :rand.uniform(9) do
        n when n <= 2 ->
          [pick(["RS+", "LS+"]) | acc] ++ [subject()] ++ pick([[], ["q", "2"], ["q", "x"]])

        3 ->
          [pick(["RS-", "LS-"]) | acc] ++ [subject()] ++ pick([[], ["q"]])

        n when n <= 5 ->
          [pick(["RMSG", "LMSG"]) | acc] ++ [subject()] ++ targets ++ [size_of(pl)]

        n when n <= 7 ->
          t = h + byte_size(pl)

          [pick(["HMSG", "HMSG"]) | acc] ++
            [subject()] ++ targets ++ ["#{pick([h, h, h + 1])}", "#{pick([t, t, t - 1])}"]

        8 ->
          [pick([~s(INFO {"route_account":"$SYS"}), "A+ ACC", "A- ACC", "PING", "PONG"])]

        _ ->
          ["PUB", "foo", "1"]
      end

    line = Enum.join(frame, sep()) <> eol()

    body =
      case hd(frame) do
        "RMSG" -> pl <> "\r\n"
        "LMSG" -> pl <> "\r\n"
        "HMSG" -> hdr <> pl <> "\r\n"
        "PUB" -> "x\r\n"
        _ -> ""
      end

    line <> body
  end
end
