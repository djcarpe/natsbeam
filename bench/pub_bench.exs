# Publish-path benchmark harness.
#
#     mix run bench/pub_bench.exs [--msacc] [--eprof] [--buffer BYTES] [--skip-e2e]
#
# Microbenchmarks the parser and the account publish path, then (when the
# `nats` CLI is on PATH) runs `nats bench pub` against a listener on a free
# port and prints the msgs/sec it reports.
#
# Baseline measured 2026-10-15 on an 8-core box, OTP 28 / Elixir 1.19,
# before any optimisation work:
#
#   | Path                                     | Per message | Ceiling   |
#   |------------------------------------------|-------------|-----------|
#   | nats-server 2.15.0, end to end           | 0.26 us     | 3.8M/s    |
#   | natsbeam, end to end (nats bench pub)    | 5.0 us      | 200K/s    |
#   | Parser.parse/2 alone                     | 2.36 us     | 423K/s    |
#   | Account.publish/7, zero subscribers      | 1.62 us     | 617K/s    |
#   | :telemetry.execute per publish           | 0.14 us     |           |
#
# Targets from the plan: parser <= 0.7 us, Account.publish (no subs) <= 0.4 us,
# Account.publish (1 sub, warm cache) <= 0.6 us, end to end >= 700K msgs/s.

Mix.ensure_application!(:runtime_tools)
Mix.ensure_application!(:tools)

defmodule PubBench do
  alias Natsbeam.Protocol.Parser

  @baseline [
    {"Parser.parse/2 alone", 2.36},
    {"Account.publish/7, zero subscribers", 1.62},
    {"Account.publish/7, one subscriber", 3.37},
    {"Account.publish_matches/8, one sub, cached", nil},
    {":telemetry.execute per publish", 0.14},
    {"end to end (nats bench pub)", 5.0}
  ]

  def main(argv) do
    {opts, _, _} =
      OptionParser.parse(argv,
        switches: [msacc: :boolean, eprof: :boolean, buffer: :integer, skip_e2e: :boolean]
      )

    {:ok, _} = Application.ensure_all_started(:natsbeam)
    results = []

    results = [{"Parser.parse/2 alone", parser_bench()} | results]
    results = [{"Account.publish/7, zero subscribers", publish_bench(0)} | results]
    results = [{"Account.publish/7, one subscriber", publish_bench(1)} | results]
    results = [{"Account.publish_matches/8, one sub, cached", publish_cached_bench(1)} | results]
    results = [{":telemetry.execute per publish", telemetry_bench()} | results]

    results =
      if opts[:skip_e2e] == true or System.find_executable("nats") == nil do
        IO.puts("(skipping end-to-end: --skip-e2e or no `nats` CLI on PATH)")
        results
      else
        [{"end to end (nats bench pub)", e2e_bench(opts)} | results]
      end

    print_table(Enum.reverse(results))
  end

  # ----------------------------------------------------------------- micro

  defp parser_bench do
    frame = "PUB prof 128\r\n" <> :binary.copy("x", 128) <> "\r\n"
    per_chunk = div(65_536, byte_size(frame))
    chunk = :binary.copy(frame, per_chunk)
    reps = 200

    {us, _} =
      :timer.tc(fn ->
        Enum.reduce(1..reps, Parser.new(), fn _, p ->
          {:ok, ops, p} = Parser.parse(p, chunk)
          length(ops) == per_chunk || raise "parser dropped frames"
          p
        end)
      end)

    us / (reps * per_chunk)
  end

  defp publish_bench(subscribers) do
    {:ok, apid, sublist} = Natsbeam.Account.ensure("$G")
    subject = "bench.pub.#{subscribers}"

    subs =
      for i <- 1..subscribers//1 do
        pid = spawn(fn -> drain() end)
        :ok = Natsbeam.Account.subscribe(apid, pid, "s#{i}", subject, nil)
        pid
      end

    # One warm-up publish so any per-process caches are populated.
    Natsbeam.Account.publish(sublist, subject, nil, nil, "x", self(), true)

    n = 300_000

    {us, _} =
      :timer.tc(fn ->
        for _ <- 1..n, do: Natsbeam.Account.publish(sublist, subject, nil, nil, "x", self(), true)
      end)

    for pid <- subs, do: Process.exit(pid, :kill)
    us / n
  end

  # The steady-state client path: matches come from the per-connection cache.
  defp publish_cached_bench(subscribers) do
    {:ok, apid, sublist} = Natsbeam.Account.ensure("$G")
    subject = "bench.cached.#{subscribers}"

    subs =
      for i <- 1..subscribers//1 do
        pid = spawn(fn -> drain() end)
        :ok = Natsbeam.Account.subscribe(apid, pid, "c#{i}", subject, nil)
        pid
      end

    matches = Natsbeam.Sublist.match(sublist, subject)
    peers = Natsbeam.Sublist.remote_peers(sublist)
    n = 300_000

    {us, _} =
      :timer.tc(fn ->
        for _ <- 1..n do
          Natsbeam.Account.publish_matches(matches, peers, subject, nil, nil, "x", self(), true)
        end
      end)

    for pid <- subs, do: Process.exit(pid, :kill)
    us / n
  end

  defp drain do
    receive do
      _ -> drain()
    end
  end

  defp telemetry_bench do
    n = 300_000

    {us, _} =
      :timer.tc(fn ->
        for _ <- 1..n do
          :telemetry.execute(
            [:natsbeam, :message, :publish],
            %{bytes: 1, deliveries: 0},
            %{account: "$G", subject: "bench"}
          )
        end
      end)

    us / n
  end

  # ------------------------------------------------------------------ e2e

  defp e2e_bench(opts) do
    {:ok, _} = Natsbeam.start_listener(name: :bench, port: 0, host: "127.0.0.1")
    {:ok, port} = Natsbeam.Listener.port(:bench)

    task =
      Task.async(fn ->
        System.cmd(
          "nats",
          ~w(-s nats://127.0.0.1:#{port} bench pub bench.e2e --msgs 800000 --size 128 --no-progress),
          stderr_to_stdout: true
        )
      end)

    pid = wait_for_client()
    Process.sleep(200)

    {%ThousandIsland.Socket{socket: sock}, _} = :sys.get_state(pid)
    if b = opts[:buffer], do: :inet.setopts(sock, buffer: b)
    IO.inspect(:inet.getopts(sock, [:buffer, :recbuf, :sndbuf]), label: "socket opts")

    if opts[:eprof] do
      :eprof.start()
      :eprof.start_profiling([pid])
      Process.sleep(2000)
      :eprof.stop_profiling()
      :eprof.analyze(:total, sort: :time)
    end

    if opts[:msacc] do
      :msacc.start(2000)
      :msacc.print()
    end

    {out, _} = Task.await(task, 300_000)
    IO.puts(out)

    case Regex.run(~r/([\d,]+) msgs\/sec/, out) do
      [_, n] ->
        rate = n |> String.replace(",", "") |> String.to_integer()
        1_000_000 / rate

      _ ->
        nil
    end
  end

  defp wait_for_client do
    case Natsbeam.clients() do
      [] ->
        Process.sleep(10)
        wait_for_client()

      [%{pid: pid} | _] ->
        pid
    end
  end

  # ---------------------------------------------------------------- output

  defp print_table(results) do
    IO.puts("")
    IO.puts(String.pad_trailing("Path", 40) <> "  baseline us   now us   msgs/s")
    IO.puts(String.duplicate("-", 78))

    for {name, now} <- results do
      base = @baseline |> List.keyfind(name, 0) |> then(fn {_, b} -> b end)

      IO.puts(
        String.pad_trailing(name, 40) <>
          "  " <>
          String.pad_leading(fmt(base), 11) <>
          "  " <>
          String.pad_leading(fmt(now), 7) <>
          "  " <> String.pad_leading(rate(now), 7)
      )
    end
  end

  defp fmt(nil), do: "-"
  defp fmt(x), do: :erlang.float_to_binary(x * 1.0, decimals: 2)
  defp rate(nil), do: "-"
  defp rate(us), do: Integer.to_string(round(1_000_000 / us))
end

PubBench.main(System.argv())
