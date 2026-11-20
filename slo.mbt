///|
pub(all) enum SloStatus {
  Met
  Missed
  Indeterminate
  NoSamples
}

///|
pub(all) struct SloReport {
  total : Int64
  passing : Interval
  required : Int64
  status : SloStatus
}

///|
// Split the multiplication so count * fraction never overflows Int64.
fn ceil_fraction(count : Int64, fraction : Int32) -> Int64 {
  let scale = 1000000L
  let f = fraction.to_int64()
  count / scale * f + (count % scale * f + scale - 1L) / scale
}

///|
/// Assess a latency threshold and required passing fraction in millionths.
pub fn Histogram::assess_slo(
  self : Histogram,
  threshold : Int64,
  target : Int32,
) -> SloReport? {
  if threshold < 0L || target < 0 || target > 1000000 {
    return None
  }
  let total = self.count()
  let required = ceil_fraction(total, target)
  let passing = self.count_at_most(threshold)
  let status = if total == 0L {
    NoSamples
  } else if passing.lower >= required {
    Met
  } else if passing.upper < required {
    Missed
  } else {
    Indeterminate
  }
  Some({ total, passing, required, status, })
}
