///|
priv enum Workload {
  Narrow
  LongTail
  Burst
}

///|
fn Workload::name(self : Workload) -> String {
  match self {
    Narrow => "narrow"
    LongTail => "long_tail"
    Burst => "burst"
  }
}

///|
fn Workload::trace(self : Workload, count : Int32) -> Array[Int64] {
  let mut state = 0x3C6EF372U
  Array::makei(count, index => {
    state = state ^ (state << 13)
    state = state ^ (state >> 17)
    state = state ^ (state << 5)
    let sample = (state % 20000U).reinterpret_as_int().to_int64()
    match self {
      Narrow => 180L + sample % 85L
      LongTail =>
        if sample % 127L == 0L {
          500000L + sample
        } else {
          50L + sample
        }
      Burst =>
        if index / 2500 % 5 == 4 {
          9900L + sample % 500L
        } else {
          sample % 2000L
        }
    }
  })
}

///|
fn require(ok : Bool, message : String) -> Unit {
  if !ok {
    abort(message)
  }
}

///|
fn check_quantiles(
  sorted : Array[Int64],
  histogram : @latency.Histogram,
  indexed : @latency.Distribution,
  queries : Array[Int32],
) -> Int64 {
  let mut max_width = 0L
  for p in queries {
    let rank = if p == 0 {
      1L
    } else {
      (sorted.length().to_int64() * p.to_int64() + 999999L) / 1000000L
    }
    let exact = sorted[rank.to_int() - 1]
    let direct = histogram.quantile(p).unwrap()
    let fast = indexed.quantile(p).unwrap()
    require(
      direct.lower == fast.lower && direct.upper == fast.upper,
      "inconsistent quantile paths",
    )
    require(
      fast.lower <= exact && exact <= fast.upper,
      "exact quantile outside bucket",
    )
    let width = fast.upper - fast.lower
    if width > max_width {
      max_width = width
    }
  }
  max_width
}

///|
fn verify_thresholds(
  sorted : Array[Int64],
  distribution : @latency.Distribution,
) -> Unit {
  for i in 0..<64 {
    let threshold = sorted[i * (sorted.length() - 1) / 63]
    let mut lo = 0
    let mut hi = sorted.length()
    while lo < hi {
      let mid = lo + (hi - lo) / 2
      if sorted[mid] <= threshold {
        lo = mid + 1
      } else {
        hi = mid
      }
    }
    let interval = distribution.count_at_most(threshold)
    require(
      interval.lower <= lo.to_int64() && lo.to_int64() <= interval.upper,
      "exact threshold count outside interval",
    )
  }
}

///|
fn bench(workload : Workload, size : Int32, round : Int32) -> Unit {
  let trace = workload.trace(size)
  let sorted = trace.copy()
  let start = @clock.monotonic_clock_start()
  sorted.sort()
  let sort_us = @clock.monotonic_clock_end(start)

  let start = @clock.monotonic_clock_start()
  let shards = Array::makei(8, _ => @latency.Histogram::new())
  for i in 0..<trace.length() {
    require(
      shards[i % shards.length()].record(trace[i]),
      "invalid generated observation",
    )
  }
  let record_us = @clock.monotonic_clock_end(start)

  let start = @clock.monotonic_clock_start()
  let merged = @latency.Histogram::new()
  let mut bytes = 0
  for shard in shards {
    let wire = shard.snapshot()
    bytes += wire.length()
    require(
      merged.merge(@latency.Histogram::from_snapshot(wire).unwrap()),
      "merge overflow",
    )
  }
  let merge_us = @clock.monotonic_clock_end(start)

  let queries = Array::makei(1001, i => i * 1000)
  let start = @clock.monotonic_clock_start()
  let mut direct_checksum = 0L
  for p in queries {
    direct_checksum += merged.quantile(p).unwrap().lower
  }
  let direct_us = @clock.monotonic_clock_end(start)

  let start = @clock.monotonic_clock_start()
  let indexed = merged.distribution()
  let mut indexed_checksum = 0L
  for p in queries {
    indexed_checksum += indexed.quantile(p).unwrap().lower
  }
  let indexed_us = @clock.monotonic_clock_end(start)
  require(direct_checksum == indexed_checksum, "query checksum mismatch")
  require(merged.count() == size.to_int64(), "lost input samples")
  let max_width = check_quantiles(sorted, merged, indexed, queries)
  verify_thresholds(sorted, indexed)
  println(
    "{\"workload\":\"\{workload.name()}\",\"size\":\{size},\"round\":\{round},\"sort_us\":\{sort_us},\"record_us\":\{record_us},\"merge_us\":\{merge_us},\"direct_us\":\{direct_us},\"indexed_us\":\{indexed_us},\"snapshot_bytes\":\{bytes},\"max_bucket_width\":\{max_width},\"checksum\":\"\{indexed_checksum}\"}",
  )
}

///|
fn main {
  for workload in [Narrow, LongTail, Burst] {
    for size in [10000, 50000] {
      for round in 0..<5 {
        bench(workload, size, round)
      }
    }
  }
}
