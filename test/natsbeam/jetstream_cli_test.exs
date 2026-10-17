defmodule Natsbeam.JetStreamCLITest do
  @moduledoc "JetStream through the official nats CLI."
  use ExUnit.Case, async: false
  import Natsbeam.TestHelpers
  @moduletag :cli

  setup do
    reset_config!()
    port = start_listener!()

    for info <- Natsbeam.streams("$G"),
        do: nats(port, ["stream", "rm", info["config"]["name"], "-f"])

    {:ok, port: port}
  end

  test "stream, consumer and kv commands", %{port: port} do
    assert nats!(port, [
             "stream",
             "add",
             "ORDERS",
             "--subjects",
             "orders.>",
             "--storage",
             "memory",
             "--defaults"
           ]) =~ "Stream ORDERS was created"

    for i <- 1..3, do: nats!(port, ["pub", "orders.new", "order #{i}"])
    assert nats!(port, ["stream", "info", "ORDERS"]) =~ "Messages: 3"
    assert nats!(port, ["stream", "get", "ORDERS", "2"]) =~ "order 2"

    assert nats!(port, [
             "consumer",
             "add",
             "ORDERS",
             "WORK",
             "--pull",
             "--ack",
             "explicit",
             "--deliver",
             "all",
             "--defaults"
           ]) =~ "Consumer ORDERS > WORK"

    out = nats!(port, ["consumer", "next", "ORDERS", "WORK", "--count", "2", "--raw"])
    assert out =~ "order 1" and out =~ "order 2"
    assert nats!(port, ["consumer", "info", "ORDERS", "WORK"]) =~ "Unprocessed Messages: 1"

    sub = nats_bg(port, ["sub", "--stream", "ORDERS", "--raw", "--count", "3"])
    assert collect(sub, ~r/order 3/) =~ "order 1"

    assert nats!(port, ["kv", "add", "CFG", "--history", "3"]) =~ "Bucket Name: CFG"
    nats!(port, ["kv", "put", "CFG", "color", "blue"])
    nats!(port, ["kv", "put", "CFG", "color", "red"])
    assert nats!(port, ["kv", "get", "CFG", "color", "--raw"]) == "red"
    assert nats!(port, ["kv", "history", "CFG", "color"]) =~ "blue"
    {out, code} = nats(port, ["kv", "create", "CFG", "color", "green"])
    assert code != 0 and out =~ "key exists"
    nats!(port, ["kv", "del", "CFG", "color", "-f"])
    {out, _} = nats(port, ["kv", "get", "CFG", "color"])
    assert out =~ "not found"
    assert nats!(port, ["kv", "ls"]) =~ "CFG"
    nats!(port, ["kv", "rm", "CFG", "-f"])

    assert nats!(port, ["stream", "purge", "ORDERS", "-f"]) =~ "Purged 3 messages"
    nats!(port, ["consumer", "rm", "ORDERS", "WORK", "-f"])
    nats!(port, ["stream", "rm", "ORDERS", "-f"])
    assert nats!(port, ["stream", "ls"]) =~ "No Streams"
  end
end
