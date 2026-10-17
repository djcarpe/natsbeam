defmodule Natsbeam.Server do
  @moduledoc """
  Server-wide identity and counters: the server's NKey (`N...` id), name,
  version, start time, and the monotonically increasing client id.
  """
  use GenServer

  defstruct [:id, :seed, :name, :version, :started_at, :counter]

  def start_link(opts), do: GenServer.start_link(__MODULE__, opts, name: __MODULE__)

  @impl true
  def init(_) do
    {seed, id} = Natsbeam.NKeys.generate(:server)
    name = Natsbeam.Config.get(:server_name) || id
    counter = :atomics.new(1, signed: false)

    info = %__MODULE__{
      id: id,
      seed: seed,
      name: name,
      version: Application.spec(:natsbeam, :vsn) |> to_string(),
      started_at: DateTime.utc_now(),
      counter: counter
    }

    :persistent_term.put(__MODULE__, info)
    {:ok, info}
  end

  @doc "Static server identity."
  @spec info() :: %__MODULE__{}
  def info, do: :persistent_term.get(__MODULE__)

  @doc "Next client id."
  @spec next_client_id() :: pos_integer()
  def next_client_id, do: :atomics.add_get(info().counter, 1, 1)

  @doc "Fresh nonce for NKey/JWT challenge-response."
  def nonce, do: Base.url_encode64(:crypto.strong_rand_bytes(16), padding: false)

  @doc "Sign bytes with the server's own NKey."
  def sign(data) do
    {:ok, sig} = Natsbeam.NKeys.sign(info().seed, data)
    sig
  end
end
