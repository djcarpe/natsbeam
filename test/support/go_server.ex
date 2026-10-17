defmodule Natsbeam.GoServer do
  @moduledoc false
  # Start a real nats-server (Go) for interop tests. Returns ports and a stop fun.

  def bin do
    System.find_executable("nats-server") ||
      Enum.find_value([Path.expand("~/Work/bin/nats-server")], &if(File.exists?(&1), do: &1))
  end

  def available?, do: bin() != nil

  @doc """
  Start nats-server with free client/leaf/cluster ports. Options:
  `:jetstream` (default true), `:domain`, `:extra` (raw config text),
  `:gateway` (`[name: "gocluster", gateways: [{"beam", "nats://127.0.0.1:port"}]]`
  adds a `gateway {}` block listening on `gateway:`).
  Returns `%{client: port, leaf: port, cluster: port, gateway: port, dir: dir, os_pid: pid, name: name}`.
  """
  def start!(opts \\ []) do
    name = Keyword.get(opts, :name, "go#{System.unique_integer([:positive])}")
    [client, leaf, cluster, gw_port] = Enum.map(1..4, fn _ -> free_port() end)
    dir = Path.join(System.tmp_dir!(), "natsbeam-go-#{name}-#{System.os_time(:microsecond)}")
    File.mkdir_p!(dir)

    js =
      if Keyword.get(opts, :jetstream, true) do
        domain = if d = Keyword.get(opts, :domain), do: ", domain: #{d}", else: ""
        "jetstream { store_dir: \"#{dir}/js\"#{domain} }"
      else
        ""
      end

    # nats-server refuses JetStream with a cluster block unless routes exist, so
    # the cluster block is opt-in (route tests run Go without JetStream).
    cluster_block =
      if Keyword.get(opts, :cluster, false),
        do:
          "cluster { name: #{Keyword.get(opts, :cluster_name, "gocluster")}, listen: 127.0.0.1:#{cluster} }",
        else: ""

    # `gateway: [name: "gocluster", gateways: [{"beam", "nats://127.0.0.1:7222"}]]`
    gateway_block =
      case Keyword.get(opts, :gateway) do
        nil ->
          ""

        gw ->
          remotes =
            for {n, url} <- Keyword.get(gw, :gateways, []),
                do: "{name: #{n}, url: \"#{url}\"}"

          extra = Keyword.get(gw, :extra, "")

          "gateway { name: #{Keyword.fetch!(gw, :name)}, listen: 127.0.0.1:#{gw_port}, " <>
            "gateways: [#{Enum.join(remotes, ", ")}] #{extra} }"
      end

    # nats-server wants an explicit system account when both leafnodes and
    # gateways are defined, so gateway servers run without a leafnode port.
    leaf_block = if Keyword.has_key?(opts, :gateway), do: "", else: "leafnodes { port: #{leaf} }"

    conf = """
    server_name: #{name}
    listen: 127.0.0.1:#{client}
    #{js}
    #{leaf_block}
    #{cluster_block}
    #{gateway_block}
    #{Keyword.get(opts, :extra, "")}
    """

    conf_path = Path.join(dir, "server.conf")
    File.write!(conf_path, conf)
    log = Path.join(dir, "server.log")

    port =
      Port.open({:spawn_executable, bin()}, [
        :binary,
        :exit_status,
        args: ["-c", conf_path, "-l", log, "-DV"]
      ])

    {:os_pid, os_pid} = Port.info(port, :os_pid)
    wait_port!(client)

    ExUnit.Callbacks.on_exit(fn ->
      stop(os_pid)
      File.rm_rf(dir)
    end)

    %{
      client: client,
      leaf: leaf,
      cluster: cluster,
      gateway: gw_port,
      dir: dir,
      os_pid: os_pid,
      name: name,
      log: log,
      port: port
    }
  end

  def stop(os_pid) do
    System.cmd("kill", [Integer.to_string(os_pid)])
    :ok
  catch
    _, _ -> :ok
  end

  def free_port do
    {:ok, s} = :gen_tcp.listen(0, [:binary, ip: {127, 0, 0, 1}])
    {:ok, port} = :inet.port(s)
    :gen_tcp.close(s)
    port
  end

  defp wait_port!(port, attempts \\ 100) do
    case :gen_tcp.connect(~c"127.0.0.1", port, [:binary], 200) do
      {:ok, s} ->
        :gen_tcp.close(s)
        :ok

      _ when attempts > 0 ->
        Process.sleep(50)
        wait_port!(port, attempts - 1)

      _ ->
        raise "nats-server did not come up on #{port}"
    end
  end
end
