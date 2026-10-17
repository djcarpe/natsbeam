defmodule Natsbeam.Auth.Identity do
  @moduledoc """
  The result of a successful authentication: who the client is, which account
  it lands in, and what it may do.
  """

  defstruct account: "$G",
            user: nil,
            kind: :none,
            nkey: nil,
            permissions: nil,
            max_payload: nil,
            max_subscriptions: nil,
            expires_at: nil,
            claims: nil,
            tags: []

  @type t :: %__MODULE__{
          account: binary(),
          user: binary() | nil,
          kind: :none | :token | :password | :nkey | :jwt | :custom,
          nkey: binary() | nil,
          permissions: Natsbeam.Permissions.t() | nil,
          max_payload: pos_integer() | nil,
          max_subscriptions: non_neg_integer() | nil,
          expires_at: integer() | nil,
          claims: map() | nil,
          tags: [binary()]
        }
end
