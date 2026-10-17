import Config

# Environment overrides, handy for running several nodes side by side:
#   NATSBEAM_PORT=4223 NATSBEAM_NAME=b elixir --sname b -S mix run --no-halt
if port = System.get_env("NATSBEAM_PORT") do
  config :natsbeam,
    listeners: [
      %{
        name: :default,
        port: String.to_integer(port),
        host: System.get_env("NATSBEAM_HOST", "0.0.0.0")
      }
    ]
end

if name = System.get_env("NATSBEAM_NAME") do
  config :natsbeam, server_name: name
end

if token = System.get_env("NATSBEAM_TOKEN") do
  config :natsbeam, auth: [token: token]
end

if dir = System.get_env("NATSBEAM_STORE_DIR") do
  config :natsbeam, jetstream: [store_dir: dir]
end

if name = System.get_env("NATSBEAM_CLUSTER_NAME"), do: config(:natsbeam, cluster_name: name)

if p = System.get_env("NATSBEAM_CLUSTER_PORT"),
  do: config(:natsbeam, cluster_port: String.to_integer(p))

if p = System.get_env("NATSBEAM_LEAF_PORT"),
  do: config(:natsbeam, leaf_port: String.to_integer(p))

if routes = System.get_env("NATSBEAM_ROUTES"),
  do: config(:natsbeam, routes: String.split(routes, ",", trim: true))

if n = System.get_env("NATSBEAM_ROUTE_POOL_SIZE"),
  do: config(:natsbeam, route_pool_size: String.to_integer(n))

if accs = System.get_env("NATSBEAM_PINNED_ACCOUNTS"),
  do: config(:natsbeam, pinned_accounts: String.split(accs, ",", trim: true))

# Comma-separated remotes, each `url[;key=value...]` with keys account,
# credentials, user, pass, token, nkey, e.g. a second remote bound to the
# system account:
#   nats-leaf://hub:7422,nats-leaf://hub:7422;account=SYS;credentials=/etc/nats/sys.creds
if remotes = System.get_env("NATSBEAM_LEAF_REMOTES") do
  parse = fn entry ->
    [url | opts] = String.split(entry, ";", trim: true)

    Enum.reduce(opts, %{url: String.trim(url)}, fn opt, acc ->
      case String.split(opt, "=", parts: 2) do
        [k, v] when k in ~w(account credentials user pass token nkey) ->
          Map.put(acc, String.to_atom(k), v)

        _ ->
          raise "NATSBEAM_LEAF_REMOTES: unknown option #{inspect(opt)}"
      end
    end)
  end

  config :natsbeam, leaf_remotes: Enum.map(String.split(remotes, ",", trim: true), parse)
end

if sys = System.get_env("NATSBEAM_SYSTEM_ACCOUNT") do
  config :natsbeam, auth: [system_account: if(sys in ["", "none"], do: nil, else: sys)]
end

if d = System.get_env("NATSBEAM_JS_DOMAIN"), do: config(:natsbeam, jetstream: [domain: d])

if p = System.get_env("NATSBEAM_GATEWAY_PORT"),
  do: config(:natsbeam, gateway_port: String.to_integer(p))

if a = System.get_env("NATSBEAM_GATEWAY_ADVERTISE"), do: config(:natsbeam, gateway_advertise: a)

# NATSBEAM_GATEWAYS=name=url,name=url
if gws = System.get_env("NATSBEAM_GATEWAYS") do
  config :natsbeam,
    gateways:
      gws
      |> String.split(",", trim: true)
      |> Enum.map(fn entry ->
        [name, url] = String.split(entry, "=", parts: 2)
        %{name: String.trim(name), url: String.trim(url)}
      end)
end

if peers = System.get_env("NATSBEAM_PEERS") do
  config :natsbeam, peers: peers |> String.split(",", trim: true) |> Enum.map(&String.to_atom/1)
end

# Test runs may execute concurrently (several worktrees); keep their stores apart.
if config_env() == :test do
  config :natsbeam,
    jetstream: [
      store_dir: Path.join(System.tmp_dir!(), "natsbeam-test-jetstream-#{System.pid()}")
    ]
end
