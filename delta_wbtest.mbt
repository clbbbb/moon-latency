///|
test "cumulative counter delta does not double count earlier observations" {
  let source = 12L
  let cumulative = Histogram::new()
  assert_true(cumulative.record(10L, count=98L))
  assert_true(cumulative.record(420L, count=2L))
  let baseline = Shard::new(source, 1L, 3L, Milliseconds, cumulative).unwrap()
  assert_true(cumulative.record(18L, count=200L))
  assert_true(cumulative.record(400L, count=3L))
  let next = Shard::new(source, 2L, 10L, Milliseconds, cumulative).unwrap()
  let delta = next.delta(baseline).unwrap()
  assert_eq(delta.identity(), (source, 2L))
  assert_eq(delta.timestamp(), 10L)
  let collector = Collector::new(Milliseconds, 10L, 2).unwrap()
  assert_true(collector.receive(delta) is Ok(Added))
  assert_true(collector.receive(delta) is Ok(Duplicate))
  assert_eq(collector.summary().count(), 203L)
  assert_eq(collector.summary().count_at_most(399L).lower, 200L)
  assert_true(collector.receive(baseline) is Ok(Added))
  assert_eq(collector.summary().snapshot(), cumulative.snapshot())
  collector.advance(20L).unwrap()
  assert_eq(collector.summary().count(), 203L)
}

///|
test "cumulative resets detect a decreased bucket even when total increases" {
  let baseline = Histogram::new()
  let reset = Histogram::new()
  assert_true(baseline.record(10L, count=2L))
  assert_true(reset.record(11L, count=3L))
  let encoded = baseline.snapshot()
  assert_true(reset.delta(baseline) is Err(BucketDecreased(10)))
  assert_eq(baseline.snapshot(), encoded)
  assert_eq(reset.count(), 3L)
  assert_eq(reset.delta(reset).unwrap().count(), 0L)
}

///|
test "cumulative shard differences validate report identities and time" {
  let base = packet(10L, 5L, 100L, Microseconds, 33L, 4L)
  let wrong_source = packet(11L, 6L, 101L, Microseconds, 33L, 5L)
  let wrong_unit = packet(10L, 6L, 101L, Milliseconds, 33L, 5L)
  let old_sequence = packet(10L, 5L, 101L, Microseconds, 33L, 5L)
  let old_time = packet(10L, 6L, 99L, Microseconds, 33L, 5L)
  assert_true(wrong_source.delta(base) is Err(SourceChanged))
  assert_true(wrong_unit.delta(base) is Err(UnitChanged))
  assert_true(old_sequence.delta(base) is Err(SequenceDidNotIncrease))
  assert_true(old_time.delta(base) is Err(TimestampWentBackward))
  let reset = packet(10L, 7L, 101L, Microseconds, 33L, 1L)
  assert_true(reset.delta(base) is Err(BucketDecreased(32)))
}

///|
test "high weighted cumulative counts retain complete Int64 precision" {
  let baseline = Histogram::new()
  let current = Histogram::new()
  assert_true(baseline.record(MAX_COUNT, count=MAX_COUNT - 10L))
  assert_true(current.record(MAX_COUNT, count=MAX_COUNT - 5L))
  assert_true(current.record(1L, count=5L))
  let result = current.delta(baseline).unwrap()
  assert_eq(result.count(), 10L)
  assert_eq(result.count_at_most(1L).lower, 5L)
  assert_eq(result.quantile(0).unwrap().lower, 1L)
  assert_eq(result.quantile(1000000).unwrap().upper, MAX_COUNT)
  assert_true(baseline.merge(result))
  assert_eq(baseline.snapshot(), current.snapshot())
}
