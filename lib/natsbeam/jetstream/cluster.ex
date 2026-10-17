defmodule Natsbeam.JetStream.Cluster do
  @moduledoc """
  Cluster plumbing for JetStream over Erlang distribution.

  * every node running JetStream joins the `:js_nodes` `:pg` group
  * stream leaders and consumers register `:global` names, so any node can
    find them and `GenServer.call` them transparently
  * replica placement picks peers from the live node list
  """
  alias Natsbeam.Config

  @pg Natsbeam.PG

  def cluster_name,
    do: Config.get(:cluster_name) || Config.get([:jetstream, :cluster_name]) || "natsbeam"

  @doc "Server name of a node (cached for remote nodes)."
  def server_name(n \\ node())

  def server_name(n) when n == node(), do: Natsbeam.Server.info().name

  def server_name(n) do
    key = {__MODULE__, :server_name, n}

    case :persistent_term.get(key, nil) do
      nil ->
        name =
          case :rpc.call(n, Natsbeam.Server, :info, [], 2_000) do
            %{name: name} -> name
            _ -> Atom.to_string(n)
          end

        :persistent_term.put(key, name)
        name

      name ->
        name
    end
  end

  @doc "Resolve a server name back to a node."
  def node_for(server_name) do
    Enum.find(nodes(), fn n -> server_name(n) == server_name end)
  end

  def join_nodes_group, do: :pg.join(@pg, :js_nodes, self())

  @doc "Nodes with JetStream running (including this one)."
  def nodes do
    @pg |> :pg.get_members(:js_nodes) |> Enum.map(&node/1) |> Enum.uniq() |> Enum.sort()
  end

  def peers, do: nodes() -- [node()]

  @doc "Pick up to `count` peer nodes not in `exclude`, least-loaded first."
  def pick_peers(count, exclude \\ []) do
    (nodes() -- exclude)
    |> Enum.sort_by(&load/1)
    |> Enum.take(count)
  end

  defp load(n) when n == node(),
    do: length(:global.registered_names()) |> then(fn _ -> local_stream_count() end)

  defp load(n),
    do:
      :rpc.call(n, __MODULE__, :local_stream_count, [], 2_000)
      |> then(&if(is_integer(&1), do: &1, else: 1_000_000))

  @doc false
  def local_stream_count do
    Registry.count_match(Natsbeam.Registry, {:stream_replica, :_, :_}, :_) +
      Enum.count(:global.registered_names(), fn
        {:js_stream, _, _} = k -> node(:global.whereis_name(k) || self()) == node()
        _ -> false
      end)
  end

  # ---------------------------------------------------------------- naming

  def stream_key(account, name), do: {:js_stream, account, name}
  def consumer_key(account, stream, name), do: {:js_consumer, account, stream, name}
  def members_group(account, name), do: {:js_members, account, name}

  def whereis(key) do
    case :global.whereis_name(key) do
      :undefined -> nil
      pid -> pid
    end
  end

  @doc "Register the calling process under a global key. Returns :yes | :no."
  def register(key), do: :global.register_name(key, self(), &:global.random_exit_name/3)
  def unregister(key), do: :global.unregister_name(key)

  def stream_names(account) do
    for {:js_stream, ^account, name} <- :global.registered_names(), do: name
  end

  def all_streams do
    for {:js_stream, account, name} <- :global.registered_names(), do: {account, name}
  end

  def consumer_names(account, stream) do
    for {:js_consumer, ^account, ^stream, name} <- :global.registered_names(), do: name
  end

  def join_members(account, name), do: :pg.join(@pg, members_group(account, name), self())
  def leave_members(account, name), do: :pg.leave(@pg, members_group(account, name), self())

  def members(account, name), do: :pg.get_members(@pg, members_group(account, name))
end
