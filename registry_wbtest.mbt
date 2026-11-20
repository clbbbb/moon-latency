///|
test "cross-service rollback isolates clocks identities and units" {
  let registry = Registry::new(capacity=2).unwrap()
  registry
  .register({
    service: "front",
    unit: Milliseconds,
    width: 10L,
    slots: 2,
    identities: 1,
  })
  .unwrap()
  registry
  .register({
    service: "queue",
    unit: Microseconds,
    width: 5L,
    slots: 2,
    identities: 2,
  })
  .unwrap()
  assert_eq(registry.names(), ["front", "queue"])
  let front = packet(1L, 1L, 0L, Milliseconds, 400L, 30L)
  let queue = packet(1L, 1L, 200L, Microseconds, 33L, 5L)
  registry.receive_bytes("front", front.encode()).unwrap() |> ignore
  assert_true(
    registry.receive_atomic([
      { service: "queue", shard: queue, },
      { service: "front", shard: queue, },
    ])
    is Err(InvalidRoute(1, RoutedFailure(UnitMismatch))),
  )
  assert_eq(registry.summary("queue").unwrap().count(), 0L)
  assert_eq(registry.summary("front").unwrap().count(), 30L)
  let receipt = registry
    .receive_atomic([
      { service: "queue", shard: queue, },
      { service: "front", shard: front, },
    ])
    .unwrap()
  assert_eq(receipt.added, 1)
  assert_eq(receipt.duplicates, 1)
  let policy = Policy::new([{ name: "p99", threshold: 100L, target: 990000, }]).unwrap()
  let view = registry.evaluate("queue", policy).unwrap()
  assert_true(view.unit is Microseconds)
  assert_true(view.objectives[0].report.status is Met)
  assert_true(
    registry.advance("queue", 0L)
    is Err(RoutedFailure(WindowFailure(ClockWentBackward))),
  )
  registry.advance("front", 0L).unwrap()
  assert_true(registry.remove("front"))
  assert_true(!registry.remove("front"))
  assert_true(registry.summary("front") is Err(UnknownService))
}

///|
test "registry caps requested resources across multiple configured services" {
  let registry = Registry::new().unwrap()
  let config = ServiceConfig::{
    service: "one",
    unit: Nanoseconds,
    width: 1L,
    slots: 1,
    identities: 4096,
  }
  registry.register(config).unwrap()
  registry.register({ ..config, service: "two", }).unwrap()
  assert_true(
    registry.register({ ..config, service: "three", }) is Err(ServiceBudget),
  )
  assert_true(registry.remove("one"))
  assert_true(
    registry.register({ ..config, service: "*", }) is Err(InvalidService),
  )
  assert_true(registry.register(config) is Ok(_))
  assert_eq(registry.names(), ["one", "two"])
}

///|
test "registry snapshot preserves service clocks and retry state independently" {
  let registry = Registry::new(capacity=4).unwrap()
  registry
  .register({
    service: "z",
    unit: Milliseconds,
    width: 100L,
    slots: 2,
    identities: 1,
  })
  .unwrap()
  registry
  .register({
    service: "a",
    unit: Microseconds,
    width: 10L,
    slots: 3,
    identities: 7,
  })
  .unwrap()
  let item = packet(100L, MAX_COUNT, 10L, Microseconds, 17L, 20L)
  registry.receive_bytes("a", item.encode()).unwrap() |> ignore
  registry.advance("z", 300L).unwrap()
  let bytes = registry.checkpoint()
  let restored = Registry::restore(bytes).unwrap()
  assert_eq(restored.names(), ["a", "z"])
  assert_eq(restored.checkpoint(), bytes)
  assert_true(restored.receive_bytes("a", item.encode()) is Ok(Duplicate))
  assert_eq(restored.summary("a").unwrap().count(), 20L)
  assert_true(
    restored.advance("z", 290L)
    is Err(RoutedFailure(WindowFailure(ClockWentBackward))),
  )
  for position in 0..<bytes.length() {
    assert_true(Registry::restore(bytes[:position].to_owned()) is Err(_))
  }
  assert_true(restored.remove("a"))
  assert_eq(registry.names(), ["a", "z"])
}
