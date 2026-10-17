import Config

config :natsbeam,
  listeners: [%{name: :default, port: 4222, host: "0.0.0.0"}]

if config_env() == :test do
  config :natsbeam, start_listeners: false

  config :natsbeam,
    jetstream: [store_dir: Path.join(System.tmp_dir!(), "natsbeam-test-jetstream")]

  config :logger, level: :warning
end
