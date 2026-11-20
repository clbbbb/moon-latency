///|
fn packet(
  source : Int64,
  sequence : Int64,
  time : Int64,
  unit : LatencyUnit,
  value : Int64,
  weight : Int64,
) -> Shard {
  let hist = Histogram::new()
  ignore(hist.record(value, count=weight))
  Shard::new(source, sequence, time, unit, hist).unwrap()
}

///|
test "collector decodes wire shards, reorders within the window, and deduplicates retries" {
  let collector = Collector::new(Milliseconds, 10L, 3, identities=2).unwrap()
  let newer = packet(1L, 42L, 30L, Milliseconds, 400L, 3L)
  let older = packet(2L, 42L, 11L, Milliseconds, 20L, 4L)
  assert_eq(Shard::decode(older.encode()).unwrap().encode(), older.encode())
  assert_true(collector.receive_bytes(newer.encode()) is Ok(Added))
  assert_true(collector.receive_bytes(older.encode()) is Ok(Added))
  assert_true(collector.receive_bytes(older.encode()) is Ok(Duplicate))
  let conflict = packet(2L, 42L, 11L, Milliseconds, 21L, 4L)
  assert_true(collector.receive(conflict) is Err(ConflictingIdentity))
  let other_unit = packet(3L, 42L, 30L, Microseconds, 21L, 4L)
  assert_true(collector.receive(other_unit) is Err(UnitMismatch))
  assert_true(
    collector.receive_bytes(b"MLS1") is Err(ShardFailure(InvalidEncoding)),
  )
  assert_eq(collector.summary().count(), 7L)
  let extra = packet(3L, 42L, 30L, Milliseconds, 21L, 4L)
  assert_true(collector.receive(extra) is Err(IdentityBudget))
  collector.advance(40L).unwrap()
  assert_eq(collector.summary().count(), 3L)
  assert_true(collector.receive(older) is Err(WindowFailure(ClockWentBackward)))
  assert_true(collector.receive(extra) is Ok(Added))
  assert_eq(collector.summary().count(), 7L)
}

///|
test "collector rejection on overflow preserves the identity budget and clock" {
  let collector = Collector::new(Nanoseconds, 1L, 2, identities=2).unwrap()
  let full = packet(1L, 1L, 0L, Nanoseconds, 10L, MAX_COUNT)
  let next = packet(1L, 2L, 1L, Nanoseconds, 10L, 1L)
  assert_true(collector.receive(full) is Ok(Added))
  assert_true(collector.receive(next) is Err(WindowFailure(CountOverflow)))
  collector.advance(0L).unwrap()
  assert_eq(collector.summary().count(), MAX_COUNT)
  assert_true(collector.receive(full) is Ok(Duplicate))
  let retry = packet(1L, 2L, 2L, Nanoseconds, 10L, 1L)
  assert_true(collector.receive(retry) is Ok(Added))
  assert_eq(collector.summary().count(), 1L)
}

///|
test "prefix index agrees with independent counts including partial shared buckets" {
  let hist = Histogram::new()
  let values = Array::makei(320, i => ((i * i * 29 + 17 * i) % 2048).to_int64())
  for value in values {
    ignore(hist.record(value))
  }
  let indexed = hist.distribution()
  for p in 0..<101 {
    let expected = hist.quantile(p * 10000).unwrap()
    let actual = indexed.quantile(p * 10000).unwrap()
    assert_eq(actual.lower, expected.lower)
    assert_eq(actual.upper, expected.upper)
  }
  for i in 0..<71 {
    let low = (i * 29).to_int64()
    let high = low + (i % 23).to_int64()
    let mut exact = 0L
    for value in values {
      if value >= low && value <= high {
        exact += 1L
      }
    }
    let result = indexed.count_between(low, high).unwrap()
    assert_true(result.lower <= exact && exact <= result.upper)
    let fast = indexed.count_at_most(high)
    let slow = hist.count_at_most(high)
    assert_eq(fast.lower, slow.lower)
    assert_eq(fast.upper, slow.upper)
  }
  ignore(hist.record(10L))
  assert_eq(indexed.count(), 320L)
}
