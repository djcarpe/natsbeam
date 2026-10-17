defmodule Natsbeam.PermissionsTest do
  use ExUnit.Case, async: true
  alias Natsbeam.Permissions

  test "publish allow/deny" do
    p = Permissions.from_map(%{publish: %{allow: ["orders.>"], deny: ["orders.admin.*"]}})
    assert Permissions.can_publish?(p, "orders.new")
    refute Permissions.can_publish?(p, "orders.admin.reset")
    refute Permissions.can_publish?(p, "other")
    assert Permissions.can_publish?(nil, "anything")
  end

  test "subscribe with wildcards and queue filters" do
    p =
      Permissions.from_map(%{
        "sub" => %{"allow" => ["orders.> workers", "events.>"], "deny" => ["events.secret.*"]}
      })

    assert Permissions.can_subscribe?(p, "orders.*", "workers")
    refute Permissions.can_subscribe?(p, "orders.*", nil)
    refute Permissions.can_subscribe?(p, "orders.*", "other")
    assert Permissions.can_subscribe?(p, "events.public", nil)
    refute Permissions.can_subscribe?(p, "events.secret.x", nil)
    refute Permissions.can_subscribe?(p, "events.>", nil)
    refute Permissions.can_subscribe?(p, ">", nil)
  end

  test "jwt style resp permissions" do
    p = Permissions.from_map(%{"resp" => %{"max" => 3, "ttl" => 5_000_000_000}})
    assert p.response == %{max: 3, ttl: 5_000}
  end
end
