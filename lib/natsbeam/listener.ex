defmodule Natsbeam.Listener do
  @moduledoc """
  A TCP (optionally TLS) listener accepting NATS clients. Listeners are
  started and stopped dynamically under `Natsbeam.ListenerSupervisor`.

  Options:

    * `:name` — identifier (default: the port)
    * `:port` — TCP port (default 4222; `0` picks a free port)
    * `:host` — bind address (default `"0.0.0.0"`)
    * `:tls` — `:ssl` options (`certfile:`, `keyfile:`, `cacertfile:`, `verify:`)
      to upgrade after `INFO` (standard NATS TLS)
    * `:tls_handshake_first` — start TLS before `INFO` (NATS `handshake_first`)
    * `:num_acceptors` — acceptor pool size (default 10)
  """

  def child_spec(opts) do
    opts = Map.new(opts)
    name = Map.get(opts, :name) || Map.get(opts, :port) || :default
    port = Map.get(opts, :port, 4222)
    host = Map.get(opts, :host, "0.0.0.0")
    {:ok, ip} = :inet.parse_address(String.to_charlist(host))
    tls = normalize_tls(Map.get(opts, :tls))
    first? = Map.get(opts, :tls_handshake_first, false) == true

    transport_opts = [
      ip: ip,
      nodelay: true,
      # Driver read buffer: each wakeup of the connection process delivers up
      # to this much (the default 9 KiB meant a wakeup every ~65 small
      # messages). 32–64 KiB measured best; larger was slower. See
      # bench/pub_bench.exs --buffer.
      buffer: 65_536,
      send_timeout: Natsbeam.Config.get(:write_timeout),
      send_timeout_close: true
    ]

    {transport_module, transport_opts} =
      if tls && first? do
        {ThousandIsland.Transports.SSL, transport_opts ++ tls}
      else
        {ThousandIsland.Transports.TCP, transport_opts}
      end

    ti_opts = [
      port: port,
      handler_module: Natsbeam.Client,
      handler_options: %{listener: name, tls: tls, tls_handshake_first: first?},
      transport_module: transport_module,
      transport_options: transport_opts,
      num_acceptors: Map.get(opts, :num_acceptors, 10),
      read_timeout: :infinity,
      shutdown_timeout: 2_000,
      silent_terminate_on_error: true,
      supervisor_options: [name: via(name)]
    ]

    %{
      id: {__MODULE__, name},
      start: {ThousandIsland, :start_link, [ti_opts]},
      type: :supervisor,
      restart: :permanent
    }
  end

  def via(name), do: {:via, Registry, {Natsbeam.Registry, {:listener, name}}}

  defp normalize_tls(nil), do: nil
  defp normalize_tls(false), do: nil

  defp normalize_tls(opts) do
    opts =
      Enum.map(opts, fn {k, v} ->
        {k,
         if(is_binary(v) and k in [:certfile, :keyfile, :cacertfile],
           do: String.to_charlist(v),
           else: v
         )}
      end)

    Keyword.put_new(opts, :verify, :verify_none)
  end

  @doc "Running listener names."
  def list do
    Registry.select(Natsbeam.Registry, [{{{:listener, :"$1"}, :_, :_}, [], [:"$1"]}])
  end

  @doc "The port a listener is bound to (useful with `port: 0`)."
  def port(name) do
    case Registry.lookup(Natsbeam.Registry, {:listener, name}) do
      [{pid, _}] ->
        case ThousandIsland.listener_info(pid) do
          {:ok, {_ip, port}} -> {:ok, port}
          other -> other
        end

      [] ->
        {:error, :not_found}
    end
  end
end
