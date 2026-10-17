defmodule Natsbeam.TLSTest do
  use ExUnit.Case, async: false
  import Natsbeam.TestHelpers
  @moduletag :tls

  setup_all do
    dir = Path.join(System.tmp_dir!(), "natsbeam-tls-#{System.unique_integer([:positive])}")
    File.mkdir_p!(dir)

    {_, 0} =
      System.cmd(
        "openssl",
        ~w(req -x509 -newkey rsa:2048 -nodes -keyout #{dir}/key.pem -out #{dir}/cert.pem -days 1 -subj /CN=localhost -addext subjectAltName=DNS:localhost,IP:127.0.0.1),
        stderr_to_stdout: true
      )

    on_exit(fn -> File.rm_rf!(dir) end)
    {:ok, dir: dir}
  end

  setup do
    reset_config!()
    :ok
  end

  test "upgrades to TLS after INFO", %{dir: dir} do
    port = start_listener!(tls: [certfile: "#{dir}/cert.pem", keyfile: "#{dir}/key.pem"])
    {sock, info} = connect(port)
    assert info["tls_required"] == true
    {:ok, ssl} = :ssl.connect(sock, [verify: :verify_none, active: false, packet: :line], 2_000)
    :ok = :ssl.send(ssl, ~s(CONNECT {"tls_required":true}\r\nPING\r\n))
    assert {:ok, "PONG\r\n"} = :ssl.recv(ssl, 0, 2_000)
    assert [%{tls: true}] = Enum.filter(Natsbeam.clients(), & &1.tls)
  end

  @tag :cli
  test "nats CLI connects over TLS", %{dir: dir} do
    port = start_listener!(tls: [certfile: "#{dir}/cert.pem", keyfile: "#{dir}/key.pem"])
    assert nats!(port, ["pub", "--tlsca", "#{dir}/cert.pem", "x", "y"]) =~ "Published"
  end
end
