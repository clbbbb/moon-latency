///|
test "shard merge matches single aggregation and contains exact quantiles" {
  let all = @moon-latency.Histogram::new()
  let left = @moon-latency.Histogram::new()
  let right = @moon-latency.Histogram::new()
  let values = Array::makei(2000, i => {
    ((i * i * 71 + i * 19) % 10007).to_int64()
  })
  for i in 0..<values.length() {
    let shard = if i % 2 == 0 { left } else { right }
    assert_true(shard.record(values[i]))
    assert_true(all.record(values[i]))
  }
  assert_true(left.merge(right))
  assert_eq(left.count(), 2000L)
  assert_eq(right.count(), 1000L)
  values.sort()
  for p in [0, 500, 250000, 500000, 950000, 990000, 1000000] {
    let rank = if p == 0 { 1 } else { (2000 * p + 999999) / 1000000 }
    let exact = values[rank - 1]
    let got = left.quantile(p).unwrap()
    let single = all.quantile(p).unwrap()
    assert_true(got.lower <= exact && exact <= got.upper)
    assert_eq(got.lower, single.lower)
    assert_eq(got.upper, single.upper)
  }
  for threshold in [-1L, 0L, 31L, 999L, 5001L, 10006L] {
    let mut exact = 0L
    for value in values {
      if value <= threshold {
        exact += 1L
      }
    }
    let got = left.count_at_most(threshold)
    assert_true(got.lower <= exact && exact <= got.upper)
  }
}

///|
test "Int64 extrema and overflow preserve valid counts" {
  let max = 9223372036854775807L
  let hist = @moon-latency.Histogram::new()
  assert_true(hist.quantile(0) is None)
  assert_true(!hist.record(-1L))
  assert_true(!hist.record(0L, count=0L))
  assert_true(hist.record(0L, count=max - 1L))
  assert_true(hist.record(max))
  assert_true(!hist.record(10L))
  assert_true(!hist.merge(hist))
  assert_eq(hist.count(), max)
  assert_eq(hist.quantile(999999).unwrap().upper, 0L)
  assert_eq(hist.quantile(1000000).unwrap().upper, max)
  assert_eq(hist.count_at_most(max).lower, max)
}
