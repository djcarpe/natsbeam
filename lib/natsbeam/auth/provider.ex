defmodule Natsbeam.Auth.Provider do
  @moduledoc """
  Behaviour for plugging custom authentication into the server at runtime.

      defmodule MyAuth do
        @behaviour Natsbeam.Auth.Provider

        def authenticate(%{"user" => u, "pass" => p}, _nonce, _client, _opts) do
          case MyApp.Accounts.verify(u, p) do
            {:ok, tenant} -> {:ok, %Natsbeam.Auth.Identity{account: tenant, user: u, kind: :custom}}
            _ -> {:error, :unauthorized}
          end
        end

        def authenticate(_, _, _, _), do: :next
      end

      Natsbeam.set_auth_provider(MyAuth, [])

  Return `:next` to fall through to the built-in authenticators.
  """

  @callback authenticate(
              connect_opts :: map(),
              nonce :: binary() | nil,
              client :: map(),
              opts :: term()
            ) :: {:ok, Natsbeam.Auth.Identity.t()} | {:error, term()} | :next
end
