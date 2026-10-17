defmodule Natsbeam.Boot do
  @moduledoc false
  # Starts the listeners named in the initial configuration once the rest of
  # the tree is up. Runs as a task so a failing port doesn't take the app down.
  use Task, restart: :temporary
  require Logger

  def start_link(_), do: Task.start_link(__MODULE__, :run, [])

  def run do
    for peer <- Application.get_env(:natsbeam, :peers, []) do
      Node.connect(peer) || Logger.warning("natsbeam: could not connect to peer #{peer}")
    end

    cfg = Natsbeam.Config.all()
    if p = cfg.cluster_port, do: Natsbeam.start_route_listener(p)
    if p = cfg.leaf_port, do: Natsbeam.start_leaf_listener(p)

    if p = cfg.gateway_port do
      opts = if h = cfg.gateway_host, do: [host: h], else: []
      Natsbeam.start_gateway_listener(p, opts)
    end

    for g <- cfg.gateways || [] do
      g = Map.new(g)
      Natsbeam.add_gateway(g[:url], Map.to_list(Map.delete(g, :url)))
    end

    for url <- cfg.routes || [], do: Natsbeam.add_route(url)

    for r <- cfg.leaf_remotes || [] do
      r = Map.new(r)
      Natsbeam.add_leaf_remote(r[:url], Map.to_list(Map.delete(r, :url)))
    end

    unless Application.get_env(:natsbeam, :start_listeners, true) == false do
      for l <- Natsbeam.Config.get(:listeners, []) do
        case Natsbeam.start_listener(l) do
          {:ok, _} ->
            {:ok, port} =
              Natsbeam.Listener.port(Map.get(l, :name) || Map.get(l, :port) || :default)

            Logger.info(
              "natsbeam: listening for clients on #{Map.get(l, :host, "0.0.0.0")}:#{port}"
            )

          {:error, reason} ->
            Logger.error("natsbeam: could not start listener #{inspect(l)}: #{inspect(reason)}")
        end
      end
    end

    :ok
  end
end
