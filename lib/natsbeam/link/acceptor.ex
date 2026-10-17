defmodule Natsbeam.Link.Acceptor do
  @moduledoc """
  Listens for inbound route (cluster port) or leafnode connections from other
  servers and hands each socket to a `Natsbeam.Link` in accepted mode (or, for
  the gateway port, to a `Natsbeam.Gateway` inbound connection).
  """
  use GenServer
  require Logger

  def start_link({kind, port, opts}) do
    GenServer.start_link(__MODULE__, {kind, port, opts}, name: via(kind))
  end

  def via(kind), do: {:via, Registry, {Natsbeam.Registry, {:link_acceptor, kind}}}

  def port(kind) do
    case Registry.lookup(Natsbeam.Registry, {:link_acceptor, kind}) do
      [{pid, _}] -> GenServer.call(pid, :port)
      [] -> {:error, :not_found}
    end
  end

  @impl true
  def init({kind, port, opts}) do
    host = Keyword.get(opts, :host, "0.0.0.0")
    {:ok, ip} = :inet.parse_address(String.to_charlist(host))

    case :gen_tcp.listen(port, [:binary, ip: ip, active: false, reuseaddr: true, nodelay: true]) do
      {:ok, lsock} ->
        {:ok, actual} = :inet.port(lsock)
        Logger.info("natsbeam: accepting #{kind} connections on #{host}:#{actual}")
        spawn_link(fn -> accept_loop(lsock, kind) end)
        {:ok, %{kind: kind, lsock: lsock, port: actual}}

      {:error, reason} ->
        {:stop, reason}
    end
  end

  @impl true
  def handle_call(:port, _from, state), do: {:reply, {:ok, state.port}, state}

  @impl true
  def terminate(_reason, state) do
    :gen_tcp.close(state.lsock)
    :ok
  end

  defp accept_loop(lsock, kind) do
    case :gen_tcp.accept(lsock) do
      {:ok, sock} ->
        case start_accepted(kind, sock) do
          {:ok, pid} ->
            :gen_tcp.controlling_process(sock, pid)
            send(pid, :socket_ready)

          _ ->
            :gen_tcp.close(sock)
        end

        accept_loop(lsock, kind)

      {:error, :closed} ->
        :ok

      {:error, _} ->
        Process.sleep(100)
        accept_loop(lsock, kind)
    end
  end

  defp start_accepted(:gateway, sock), do: Natsbeam.Gateway.start_accepted(sock)
  defp start_accepted(kind, sock), do: Natsbeam.Link.start_accepted(kind, sock)
end
