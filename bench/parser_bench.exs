# Parser microbenchmark: new parser vs the legacy parser it replaced.
#
#     mix run --no-start bench/parser_bench.exs
#
# Baseline (legacy parser, 2026-10-15): 2.36 us/msg for plain PUB frames.
# Target: <= 0.7 us/msg.

unless Code.ensure_loaded?(Natsbeam.Protocol.LegacyParser) do
  Code.require_file("test/support/legacy_parser.ex")
end

defmodule ParserBench do
  @reps 200

  def run do
    body = :binary.copy("x", 128)
    hdr = "NATS/1.0\r\nNats-Msg-Id: 123\r\n\r\n"

    cases = [
      {"PUB subject 128B", "PUB prof 128\r\n" <> body <> "\r\n"},
      {"PUB subject reply 128B", "PUB prof _INBOX.abcdefghijklmnop 128\r\n" <> body <> "\r\n"},
      {"HPUB subject hdr 128B",
       "HPUB prof #{byte_size(hdr)} #{byte_size(hdr) + 128}\r\n" <> hdr <> body <> "\r\n"}
    ]

    IO.puts(String.pad_trailing("frame", 26) <> "   legacy us/msg   new us/msg   new msgs/s   speedup")
    IO.puts(String.duplicate("-", 86))

    for {name, frame} <- cases do
      per_chunk = div(65_536, byte_size(frame))
      chunk = :binary.copy(frame, per_chunk)
      legacy = bench(Natsbeam.Protocol.LegacyParser, chunk, per_chunk)
      new = bench(Natsbeam.Protocol.Parser, chunk, per_chunk)

      IO.puts(
        String.pad_trailing(name, 26) <>
          String.pad_leading(fmt(legacy), 16) <>
          String.pad_leading(fmt(new), 13) <>
          String.pad_leading(Integer.to_string(round(1_000_000 / new)), 13) <>
          String.pad_leading(fmt(legacy / new) <> "x", 10)
      )
    end
  end

  defp bench(mod, chunk, per_chunk) do
    # warm up
    {:ok, _, _} = mod.parse(mod.new(), chunk)

    {us, _} =
      :timer.tc(fn ->
        Enum.reduce(1..@reps, mod.new(), fn _, p ->
          {:ok, ops, p} = mod.parse(p, chunk)
          length(ops) == per_chunk || raise "#{inspect(mod)} dropped frames"
          p
        end)
      end)

    us / (@reps * per_chunk)
  end

  defp fmt(x), do: :erlang.float_to_binary(x * 1.0, decimals: 2)
end

ParserBench.run()
