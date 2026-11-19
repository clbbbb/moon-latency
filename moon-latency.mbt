// Main package file for your library code.
// Put public APIs and shared helpers here.
// Keep blocks small and focused for easy refactors.

///|
pub(all) struct Interval {
  lower : Int64
  upper : Int64
}

///|
pub struct Histogram {
  priv counts : Array[Int64]
  priv mut total : Int64
}

///|
// Integer values below 32 are exact. Each subsequent octave has 16 bins.
let bounds : Array[Int64] = {
  let result = Array::makei(32, i => i.to_int64())
  for exponent in 5..<63 {
    let base = 1L << exponent
    let step = base / 16L
    for sub in 0..<16 {
      result.push(base + sub.to_int64() * step)
    }
  }
  result
}

///|
const MAX_COUNT : Int64 = 9223372036854775807L

///|
pub fn Histogram::new() -> Histogram {
  { counts: Array::make(bounds.length(), 0L), total: 0L, }
}

///|
pub fn Histogram::count(self : Histogram) -> Int64 {
  self.total
}

///|
fn bucket(value : Int64) -> Int32 {
  let mut lo = 0
  let mut hi = bounds.length()
  while lo + 1 < hi {
    let mid = lo + (hi - lo) / 2
    if bounds[mid] <= value {
      lo = mid
    } else {
      hi = mid
    }
  }
  lo
}

///|
fn upper_at(id : Int32) -> Int64 {
  if id + 1 == bounds.length() {
    MAX_COUNT
  } else {
    bounds[id + 1] - 1L
  }
}

///|
pub fn Histogram::record(
  self : Histogram,
  value : Int64,
  count~ : Int64 = 1L,
) -> Bool {
  if value < 0L || count <= 0L || count > MAX_COUNT - self.total {
    return false
  }
  let id = bucket(value)
  self.counts[id] += count
  self.total += count
  true
}

///|
pub fn Histogram::merge(self : Histogram, other : Histogram) -> Bool {
  if other.total > MAX_COUNT - self.total {
    return false
  }
  for id in 0..<bounds.length() {
    self.counts[id] += other.counts[id]
  }
  self.total += other.total
  true
}

///|
/// Nearest-rank quantile. p is in millionths, from 0 (minimum) to 1,000,000.
pub fn Histogram::quantile(self : Histogram, p : Int32) -> Interval? {
  if p < 0 || p > 1000000 || self.total == 0L {
    return None
  }
  let fraction = p.to_int64()
  let rank = if p == 0 {
    1L
  } else {
    self.total / 1000000L * fraction +
    (self.total % 1000000L * fraction + 999999L) / 1000000L
  }
  let mut prefix = 0L
  for id in 0..<bounds.length() {
    prefix += self.counts[id]
    if prefix >= rank {
      return Some({ lower: bounds[id], upper: upper_at(id), })
    }
  }
  None
}

///|
/// Return lower and upper bounds on the number of observations <= threshold.
pub fn Histogram::count_at_most(
  self : Histogram,
  threshold : Int64,
) -> Interval {
  if threshold < 0L {
    return { lower: 0L, upper: 0L, }
  }
  let last = bucket(threshold)
  let mut lower = 0L
  for id in 0..<last {
    lower += self.counts[id]
  }
  let upper = lower + self.counts[last]
  { lower: if threshold == upper_at(last) { upper } else { lower }, upper, }
}
