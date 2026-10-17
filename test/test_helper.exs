excluded =
  [:cli, :tls, :cluster, :go]
  |> Enum.reject(fn
    :cluster -> Natsbeam.ClusterHelpers.available?()
    :go -> Natsbeam.GoServer.available?() and Natsbeam.TestHelpers.available?(:cli)
    tag -> Natsbeam.TestHelpers.available?(tag)
  end)

ExUnit.start(exclude: excluded)
