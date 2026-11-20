///|
test "sparse snapshots are canonical and reject malformed inputs" {
  let hist = Histogram::new()
  assert_true(hist.record(0L, count=17L))
  assert_true(hist.record(410L, count=25L))
  assert_true(hist.record(MAX_COUNT, count=300L))
  let encoded = hist.snapshot()
  assert_eq(encoded.length(), 34)
  assert_eq(Histogram::from_snapshot(encoded).unwrap().snapshot(), encoded)
  assert_eq(Histogram::from_snapshot(b"MLH1").unwrap().count(), 0L)
  assert_true(Histogram::from_snapshot(b"MLH0") is Err(InvalidHeader))
  assert_true(
    Histogram::from_snapshot(encoded[:33].to_owned()) is Err(InvalidSize),
  )
  assert_true(
    Histogram::from_snapshot(b"MLH1\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00")
    is Err(InvalidCount),
  )
  assert_true(
    Histogram::from_snapshot(b"MLH1\xff\xff\x00\x00\x00\x00\x00\x00\x00\x01")
    is Err(InvalidBucketOrder),
  )
}

///|
test "SLO classification respects the uncertain bucket and empty input" {
  let hist = Histogram::new()
  assert_true(hist.assess_slo(400L, 990000) is Some({ status: NoSamples, .. }))
  assert_true(hist.record(25L, count=194L))
  assert_true(hist.record(410L, count=6L))
  assert_true(
    hist.assess_slo(400L, 990000)
    is Some({ status: Indeterminate, required: 198L, .. }),
  )
  assert_true(hist.assess_slo(399L, 990000) is Some({ status: Missed, .. }))
  assert_true(hist.assess_slo(415L, 1000000) is Some({ status: Met, .. }))
}

///|
test "moving window matches rebuilding by timestamp including ring reuse and clock jumps" {
  for slots in [1, 2, 7, 32] {
    let window = Window::new(5L, slots).unwrap()
    let events : Array[(Int64, Int64)] = []
    let mut time = 0L
    for i in 0..<240 {
      time += (if i % 37 == 0 { 137 } else { i % 4 }).to_int64()
      let value = (i * 7919 % 10171).to_int64()
      let shard = Histogram::new()
      assert_true(shard.record(value))
      events.push((time, value))
      window.merge_at(time, shard).unwrap()
      let expected = Histogram::new()
      for (timestamp, sample) in events {
        if time / 5L - timestamp / 5L < slots.to_int64() {
          assert_true(expected.record(sample))
        }
      }
      assert_eq(window.summary().snapshot(), expected.snapshot())
    }
    window.advance(MAX_COUNT).unwrap()
    assert_eq(window.summary().count(), 0L)
    assert_true(window.advance(0L) is Err(ClockWentBackward))
  }
}

///|
test "window overflow leaves time unchanged and expiry frees capacity" {
  let full = Histogram::new()
  let one = Histogram::new()
  assert_true(full.record(0L, count=MAX_COUNT))
  assert_true(one.record(10L))
  let window = Window::new(1L, 2).unwrap()
  window.merge_at(0L, full).unwrap()
  assert_true(window.merge_at(1L, one) is Err(CountOverflow))
  window.advance(0L).unwrap()
  assert_eq(window.summary().count(), MAX_COUNT)
  window.merge_at(2L, one).unwrap()
  assert_eq(window.summary().snapshot(), one.snapshot())
}
