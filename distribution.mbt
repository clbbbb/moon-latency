///|
/// Immutable prefix counts for repeated queries over one observation set.
pub struct Distribution {
  priv prefix : Array[Int64]
  priv total : Int64
}

///|
pub fn Histogram::distribution(self : Histogram) -> Distribution {
  let mut count = 0L
  let prefix = Array::makei(bounds.length(), i => {
    count += self.counts[i]
    count
  })
  { prefix, total: self.total, }
}

///|
pub fn Distribution::count(self : Distribution) -> Int64 {
  self.total
}

///|
pub fn Distribution::quantile(self : Distribution, p : Int32) -> Interval? {
  if p < 0 || p > 1000000 || self.total == 0L {
    return None
  }
  let rank = if p == 0 { 1L } else { ceil_fraction(self.total, p) }
  let mut lo = 0
  let mut hi = self.prefix.length() - 1
  while lo < hi {
    let mid = lo + (hi - lo) / 2
    if self.prefix[mid] < rank {
      lo = mid + 1
    } else {
      hi = mid
    }
  }
  Some({ lower: bounds[lo], upper: upper_at(lo), })
}

///|
pub fn Distribution::count_at_most(
  self : Distribution,
  threshold : Int64,
) -> Interval {
  if threshold < 0L {
    return { lower: 0L, upper: 0L, }
  }
  let index = bucket(threshold)
  let upper = self.prefix[index]
  let lower = if threshold == upper_at(index) {
    upper
  } else if index == 0 {
    0L
  } else {
    self.prefix[index - 1]
  }
  { lower, upper, }
}

///|
/// Count observations in an inclusive interval without counting the same
/// partially covered bucket twice in the upper bound.
pub fn Distribution::count_between(
  self : Distribution,
  lower : Int64,
  upper : Int64,
) -> Interval? {
  if lower < 0L || upper < lower {
    return None
  }
  let first = bucket(lower)
  let last = bucket(upper)
  let before = if first == 0 { 0L } else { self.prefix[first - 1] }
  let possible = self.prefix[last] - before
  let exact_start = if lower == bounds[first] {
    before
  } else {
    self.prefix[first]
  }
  let exact_end = if upper == upper_at(last) {
    self.prefix[last]
  } else if last == 0 {
    0L
  } else {
    self.prefix[last - 1]
  }
  Some({
    lower: if exact_end < exact_start {
      0L
    } else {
      exact_end - exact_start
    },
    upper: possible,
  })
}
