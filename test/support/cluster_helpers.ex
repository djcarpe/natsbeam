defmodule Natsbeam.ClusterHelpers do
  @moduledoc false
  # Spin up extra BEAM nodes running natsbeam (no TCP listeners) for cluster tests.

  def available? do
    System.find_executable("epmd") != nil and System.find_executable("erl") != nil
  end

  def ensure_distributed! do
    unless Node.alive?() do
      System.cmd("epmd", ["-daemon"])
      {:ok, _} = :net_kernel.start([:"natsbeam_test@127.0.0.1", :longnames])
    end

    :ok
  end

  @doc "Start a peer node running natsbeam with its own store dir. Returns `{peer_pid, node}`."
  def start_peer!(name) do
    ensure_distributed!()
    paths = :code.get_path() |> Enum.flat_map(&[~c"-pa", &1])

    {:ok, pid, node} =
      :peer.start_link(%{
        name: name,
        host: ~c"127.0.0.1",
        longnames: true,
        connection: :standard_io,
        args:
          paths ++
            [~c"-setcookie", Atom.to_charlist(Node.get_cookie()), ~c"-connect_all", ~c"true"]
      })

    true = Node.connect(node)

    store =
      Path.join(System.tmp_dir!(), "natsbeam-peer-#{name}-#{System.os_time(:microsecond)}")

    File.rm_rf!(store)
    ExUnit.Callbacks.on_exit(fn -> File.rm_rf(store) end)

    for {k, v} <- [
          start_listeners: false,
          listeners: [],
          server_name: Atom.to_string(name),
          jetstream: [store_dir: store]
        ] do
      :peer.call(pid, Application, :put_env, [:natsbeam, k, v])
    end

    :peer.call(pid, Application, :put_env, [:logger, :level, :warning])
    {:ok, _} = :peer.call(pid, Application, :ensure_all_started, [:natsbeam])
    {pid, node}
  end

  def stop_peer(pid) do
    :peer.stop(pid)
  catch
    :exit, _ -> :ok
  end

  def call(node, m, f, a), do: :rpc.call(node, m, f, a, 10_000)
end
