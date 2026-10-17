defmodule Natsbeam.TestHelpers do
  @moduledoc false
  import ExUnit.Assertions

  @bin_dirs [Path.expand("~/Work/bin"), "/usr/local/bin", "/usr/bin"]

  def available?(:cli), do: nats_bin() != nil
  def available?(:tls), do: System.find_executable("openssl") != nil
  def available?(_), do: true

  def nats_bin do
    System.find_executable("nats") ||
      Enum.find_value(@bin_dirs, fn d ->
        p = Path.join(d, "nats")
        if File.exists?(p), do: p
      end)
  end

  @doc "Reset live config to application defaults (kicks nobody; tests isolate by account)."
  def reset_config! do
    Natsbeam.Config.update(fn _ -> Natsbeam.Config.from_application_env() end)
  end

  @doc "Start a listener on a free port; returns the port. Stopped on test exit."
  def start_listener!(opts \\ []) do
    name = Keyword.get(opts, :name, make_ref())

    {:ok, _} =
      Natsbeam.start_listener(Keyword.merge([name: name, port: 0, host: "127.0.0.1"], opts))

    {:ok, port} = Natsbeam.Listener.port(name)
    ExUnit.Callbacks.on_exit(fn -> Natsbeam.stop_listener(name) end)
    port
  end

  # ------------------------------------------------------------ raw socket

  def connect(port, opts \\ []) do
    {:ok, sock} =
      :gen_tcp.connect(~c"127.0.0.1", port, [:binary, active: false, packet: :line] ++ opts)

    {:ok, "INFO " <> json} = :gen_tcp.recv(sock, 0, 2_000)
    {sock, Jason.decode!(json)}
  end

  def send!(sock, data), do: :ok = :gen_tcp.send(sock, data)

  def recv(sock, timeout \\ 2_000) do
    case :gen_tcp.recv(sock, 0, timeout) do
      {:ok, line} -> String.trim_trailing(line, "\r\n")
      {:error, reason} -> {:error, reason}
    end
  end

  def recv_msg(sock, timeout \\ 2_000) do
    case recv(sock, timeout) do
      "MSG " <> rest ->
        parts = String.split(rest, " ")

        {subject, sid, reply, size} =
          case parts do
            [s, i, n] -> {s, i, nil, n}
            [s, i, r, n] -> {s, i, r, n}
          end

        payload = recv_payload(sock, String.to_integer(size), timeout)
        %{op: :msg, subject: subject, sid: sid, reply: reply, payload: payload, headers: nil}

      "HMSG " <> rest ->
        parts = String.split(rest, " ")

        {subject, sid, reply, hlen, total} =
          case parts do
            [s, i, h, t] -> {s, i, nil, h, t}
            [s, i, r, h, t] -> {s, i, r, h, t}
          end

        total = String.to_integer(total)
        hlen = String.to_integer(hlen)
        body = recv_payload(sock, total, timeout)
        <<headers::binary-size(hlen), payload::binary>> = body
        %{op: :hmsg, subject: subject, sid: sid, reply: reply, payload: payload, headers: headers}

      other ->
        other
    end
  end

  # Payloads may contain newlines; switch to raw mode for exactly `size + 2` bytes.
  defp recv_payload(sock, size, timeout) do
    :inet.setopts(sock, packet: :raw)
    {:ok, data} = :gen_tcp.recv(sock, size + 2, timeout)
    :inet.setopts(sock, packet: :line)
    binary_part(data, 0, size)
  end

  def connect!(port, connect_opts \\ %{}) do
    {sock, info} = connect(port)

    send!(
      sock,
      "CONNECT " <>
        Jason.encode!(Map.merge(%{"verbose" => false}, connect_opts)) <> "\r\nPING\r\n"
    )

    assert "PONG" = recv(sock)
    {sock, info}
  end

  # ------------------------------------------------------------------- cli

  def nats(port, args, opts \\ []) do
    env = Keyword.get(opts, :env, [])

    {out, code} =
      System.cmd(nats_bin(), ["-s", "nats://127.0.0.1:#{port}" | args],
        stderr_to_stdout: true,
        env: env
      )

    {String.trim(out), code}
  end

  def nats!(port, args, opts \\ []) do
    {out, code} = nats(port, args, opts)
    assert code == 0, "nats #{Enum.join(args, " ")} failed (#{code}): #{out}"
    out
  end

  @doc "Run `nats` in the background; returns an Erlang port. Output collects via `collect/2`."
  def nats_bg(port, args) do
    Port.open({:spawn_executable, nats_bin()}, [
      :binary,
      :exit_status,
      :stderr_to_stdout,
      args: ["-s", "nats://127.0.0.1:#{port}" | args]
    ])
  end

  @doc "Wait until the background process's output matches `pattern` (or it exits)."
  def collect(port, pattern, timeout \\ 5_000, acc \\ "") do
    if acc =~ pattern do
      acc
    else
      receive do
        {^port, {:data, data}} -> collect(port, pattern, timeout, acc <> data)
        {^port, {:exit_status, _}} -> acc
      after
        timeout -> acc
      end
    end
  end

  def wait_until(fun, timeout \\ 2_000) do
    deadline = System.monotonic_time(:millisecond) + timeout
    do_wait(fun, deadline)
  end

  defp do_wait(fun, deadline) do
    if fun.() do
      true
    else
      if System.monotonic_time(:millisecond) > deadline do
        false
      else
        Process.sleep(20)
        do_wait(fun, deadline)
      end
    end
  end

  # ------------------------------------------------------------------- jwt

  @doc "Generate an operator, an account and a user; returns a map of seeds, keys and JWTs."
  def operator_fixture(opts \\ []) do
    {op_seed, op_pub} = Natsbeam.NKeys.generate(:operator)
    {acc_seed, acc_pub} = Natsbeam.NKeys.generate(:account)
    {usr_seed, usr_pub} = Natsbeam.NKeys.generate(:user)

    {:ok, op_jwt} =
      Natsbeam.JWT.sign(
        %{"sub" => op_pub, "name" => "OP", "nats" => %{"type" => "operator", "version" => 2}},
        op_seed
      )

    account_nats =
      Map.merge(
        %{"type" => "account", "version" => 2, "limits" => %{}},
        Keyword.get(opts, :account, %{})
      )

    {:ok, acc_jwt} =
      Natsbeam.JWT.sign(%{"sub" => acc_pub, "name" => "ACC", "nats" => account_nats}, op_seed)

    user_nats = Map.merge(%{"type" => "user", "version" => 2}, Keyword.get(opts, :user, %{}))
    user_claims = %{"sub" => usr_pub, "name" => "user", "nats" => user_nats}

    user_claims =
      if exp = Keyword.get(opts, :exp), do: Map.put(user_claims, "exp", exp), else: user_claims

    {:ok, usr_jwt} = Natsbeam.JWT.sign(user_claims, acc_seed)

    %{
      op_seed: op_seed,
      op_pub: op_pub,
      op_jwt: op_jwt,
      acc_seed: acc_seed,
      acc_pub: acc_pub,
      acc_jwt: acc_jwt,
      usr_seed: usr_seed,
      usr_pub: usr_pub,
      usr_jwt: usr_jwt
    }
  end

  def creds_file(jwt, seed) do
    path = Path.join(System.tmp_dir!(), "natsbeam-#{System.unique_integer([:positive])}.creds")

    File.write!(path, """
    -----BEGIN NATS USER JWT-----
    #{jwt}
    ------END NATS USER JWT------

    ************************* IMPORTANT *************************
    NKEY Seed printed below can be used to sign and prove identity.
    NKEYs are sensitive and should be treated as secrets.

    -----BEGIN USER NKEY SEED-----
    #{seed}
    ------END USER NKEY SEED------
    """)

    ExUnit.Callbacks.on_exit(fn -> File.rm(path) end)
    path
  end

  def sign_nonce(seed, nonce) do
    {:ok, sig} = Natsbeam.NKeys.sign(seed, nonce)
    Base.url_encode64(sig, padding: false)
  end
end
