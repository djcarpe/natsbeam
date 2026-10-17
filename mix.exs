defmodule Natsbeam.MixProject do
  use Mix.Project

  def project do
    [
      app: :natsbeam,
      version: "0.1.0",
      elixir: "~> 1.15",
      start_permanent: Mix.env() == :prod,
      deps: deps(),
      elixirc_paths: elixirc_paths(Mix.env()),
      description:
        "A NATS-compatible messaging server written in Elixir, with BEAM-style runtime dynamism."
    ]
  end

  def application do
    [
      extra_applications: [:logger, :crypto, :ssl, :public_key],
      mod: {Natsbeam.Application, []}
    ]
  end

  defp elixirc_paths(:test), do: ["lib", "test/support"]
  defp elixirc_paths(_), do: ["lib"]

  defp deps do
    [
      {:thousand_island, "~> 1.5"},
      {:jason, "~> 1.4"},
      {:telemetry, "~> 1.2"},
      {:bcrypt_elixir, "~> 3.0"}
    ]
  end
end
