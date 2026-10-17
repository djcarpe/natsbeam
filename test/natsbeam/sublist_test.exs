defmodule Natsbeam.SublistTest do
  use ExUnit.Case, async: true
  alias Natsbeam.Sublist

  setup do
    # tables die with the test process
    {:ok, t: Sublist.new("test")}
  end

  test "matches literals and wildcards", %{t: t} do
    me = self()
    Sublist.insert(t, me, "1", "foo.bar", nil)
    Sublist.insert(t, me, "2", "foo.*", nil)
    Sublist.insert(t, me, "3", "foo.>", "q")
    Sublist.insert(t, me, "4", ">", nil)
    Sublist.insert(t, me, "5", "bar.*", nil)

    sids = fn subject -> t |> Sublist.match(subject) |> Enum.map(&elem(&1, 1)) |> Enum.sort() end
    assert sids.("foo.bar") == ["1", "2", "3", "4"]
    assert sids.("foo.baz") == ["2", "3", "4"]
    assert sids.("foo.bar.baz") == ["3", "4"]
    assert sids.("foo") == ["4"]
    assert sids.("bar.x") == ["4", "5"]
  end

  test "removal prunes the trie", %{t: t} do
    me = self()
    Sublist.insert(t, me, "1", "a.b.c", nil)
    Sublist.insert(t, me, "2", "a.b.d", nil)
    assert Sublist.remove(t, me, "1")
    assert [{^me, "2", nil}] = Sublist.match(t, "a.b.d")
    assert [] = Sublist.match(t, "a.b.c")
    assert 1 == Sublist.remove_all(t, me)
    assert Sublist.count(t) == 0
    assert :ets.info(t.children, :size) == 0
  end

  test "every write bumps the generation", %{t: t} do
    me = self()
    g0 = Sublist.generation(t)
    Sublist.insert(t, me, "1", "a.b", nil)
    g1 = Sublist.generation(t)
    assert g1 > g0
    Sublist.insert(t, me, "2", "a.*", "q")
    g2 = Sublist.generation(t)
    assert g2 > g1
    assert Sublist.remove(t, me, "1")
    g3 = Sublist.generation(t)
    assert g3 > g2
    # Removing nothing changes nothing.
    refute Sublist.remove(t, me, "nope")
    assert Sublist.generation(t) == g3
    assert 0 == Sublist.remove_all(t, spawn(fn -> :ok end))
    assert Sublist.generation(t) == g3
    assert 1 == Sublist.remove_all(t, me)
    assert Sublist.generation(t) > g3
    assert Sublist.remote_peers(t) == []
  end

  test "queue groups deliver once per group" do
    t = Sublist.new("q")
    me = self()
    Sublist.insert(t, me, "a", "x", "g1")
    Sublist.insert(t, me, "b", "x", "g1")
    Sublist.insert(t, me, "c", "x", nil)
    {n, ["g1"]} = Natsbeam.Account.deliver(Sublist.match(t, "x"), "x", nil, nil, "p")
    assert n == 2
    assert_receive {:natsbeam_msg, "c", "x", nil, nil, "p"}
    assert_receive {:natsbeam_msg, sid, "x", nil, nil, "p"} when sid in ["a", "b"]
    refute_receive {:natsbeam_msg, _, _, _, _, _}
  end
end
