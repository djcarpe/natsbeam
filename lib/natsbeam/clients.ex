defmodule Natsbeam.Clients do
  @moduledoc "Queries over connected clients (socket and in-VM alike)."

  @registry Natsbeam.ClientRegistry

  @doc "Every registered client's metadata."
  @spec list() :: [map()]
  def list do
    Registry.select(@registry, [{{:_, :"$1", :"$2"}, [], [{{:"$1", :"$2"}}]}])
    |> Enum.map(fn {pid, meta} -> live_meta(pid, meta) end)
    |> Enum.sort_by(& &1.id)
  end

  @doc "Metadata for one client id."
  def get(id) do
    case Registry.lookup(@registry, id) do
      [{pid, meta}] -> live_meta(pid, meta)
      [] -> nil
    end
  end

  def count, do: Registry.count(@registry)

  @doc "Clients matching a predicate on their metadata."
  def filter(fun), do: Enum.filter(list(), fun)

  @doc "Disconnect a client (by id or pid) with a `-ERR` reason."
  def disconnect(id_or_pid, reason \\ "Connection Closed by Server")

  def disconnect(pid, reason) when is_pid(pid) do
    send(pid, {:natsbeam_close, reason})
    :ok
  end

  def disconnect(id, reason) do
    case Registry.lookup(@registry, id) do
      [{pid, _}] -> disconnect(pid, reason)
      [] -> {:error, :not_found}
    end
  end

  # Metadata in the registry is a snapshot from the last register/update; ask
  # socket clients for their live view when they are alive.
  defp live_meta(pid, meta) do
    if meta[:kind] != :local and Process.alive?(pid) do
      try do
        GenServer.call(pid, :info, 1_000)
      catch
        :exit, _ -> meta
      end
    else
      meta
    end
  end
end
