defmodule Natsbeam.SubjectTest do
  use ExUnit.Case, async: true
  alias Natsbeam.Subject

  test "matches literal and wildcard patterns" do
    assert Subject.matches?("foo.bar", "foo.bar")
    refute Subject.matches?("foo.bar", "foo.baz")
    assert Subject.matches?("foo.*", "foo.bar")
    refute Subject.matches?("foo.*", "foo.bar.baz")
    assert Subject.matches?("foo.>", "foo.bar.baz")
    refute Subject.matches?("foo.>", "foo")
    assert Subject.matches?(">", "anything.at.all")
    assert Subject.matches?("*.*.baz", "a.b.baz")
  end

  test "validates publish and subscribe subjects" do
    assert Subject.valid_publish?("foo.bar")
    refute Subject.valid_publish?("")
    refute Subject.valid_publish?("foo..bar")
    refute Subject.valid_publish?("foo bar")
    assert Subject.valid_publish?("foo.*")
    refute Subject.valid_publish?("foo.*", true)
    refute Subject.valid_publish?("foo.>", true)
    assert Subject.valid_publish?("foo.b*r", true)
    refute Subject.valid_publish?(".foo")
    refute Subject.valid_publish?("foo.")
    refute Subject.valid_publish?(".")
    refute Subject.valid_publish?("a..b")
    refute Subject.valid_publish?("a\tb")
    refute Subject.valid_publish?("a\r\nb")
    assert Subject.valid_publish?("a")
    assert Subject.valid_publish?("$SYS.REQ.SERVER.PING")

    assert Subject.valid_subscription?("foo.*.>")
    refute Subject.valid_subscription?("foo.>.bar")
    refute Subject.valid_subscription?("foo.b*r")
    refute Subject.valid_subscription?(".foo")
  end

  test "subset and intersection" do
    assert Subject.subset?("foo.bar", "foo.>")
    assert Subject.subset?("foo.*", "foo.>")
    assert Subject.subset?("foo.*", "*.*")
    refute Subject.subset?("foo.>", "foo.*")
    refute Subject.subset?("*", "foo")
    assert Subject.intersects?("foo.*", "*.bar")
    assert Subject.intersects?("foo.>", "foo.bar.baz")
    refute Subject.intersects?("foo.*", "bar.*")
  end
end
