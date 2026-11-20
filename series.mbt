///|
pub(all) struct EpochReport {
  epoch : Int64
  started_at : Int64
  result : SloReport
}

///|
/// Report all live epochs, including gaps with no samples. Results are ordered
/// oldest first. The final epoch can still accept shard updates.
pub fn Window::series(
  self : Window,
  threshold : Int64,
  target : Int32,
) -> Array[EpochReport]? {
  if threshold < 0L || target < 0 || target > 1000000 {
    return None
  }
  let result = []
  if self.latest < 0L {
    return Some(result)
  }
  let span = self.slots.length().to_int64()
  let oldest = if self.latest < span { 0L } else { self.latest - span + 1L }
  let zero = SloReport::{
    total: 0L,
    passing: { lower: 0L, upper: 0L, },
    required: 0L,
    status: NoSamples,
  }
  let mut epoch = oldest
  while true {
    let slot = self.slots[(epoch % span).to_int()]
    let report = if slot.epoch == epoch {
      slot.histogram.assess_slo(threshold, target).unwrap()
    } else {
      zero
    }
    result.push({ epoch, started_at: epoch * self.width, result: report, })
    if epoch == self.latest {
      break
    }
    epoch += 1L
  }
  Some(result)
}

///|
pub fn Collector::series(
  self : Collector,
  threshold : Int64,
  target : Int32,
) -> Array[EpochReport]? {
  self.window.series(threshold, target)
}

///|
pub(all) enum LatencyChange {
  Slower
  Faster
  Uncertain
  ExactTie
  MissingSamples
}

///|
pub(all) struct QuantileComparison {
  baseline : Interval?
  current : Interval?
  change : LatencyChange
  // Bounds on current - baseline, in the original latency unit.
  difference : Interval?
}

///|
/// Compare nearest-rank latency intervals without assuming an ordering when
/// the two sets admit overlapping quantile values.
pub fn compare_quantile(
  baseline : Distribution,
  current : Distribution,
  percentile : Int32,
) -> QuantileComparison? {
  if percentile < 0 || percentile > 1000000 {
    return None
  }
  let a = baseline.quantile(percentile)
  let b = current.quantile(percentile)
  match (a, b) {
    (Some(old), Some(new)) => {
      let lower = new.lower - old.upper
      let upper = new.upper - old.lower
      let change = if lower > 0L {
        Slower
      } else if upper < 0L {
        Faster
      } else if lower == 0L && upper == 0L {
        ExactTie
      } else {
        Uncertain
      }
      Some({
        baseline: a,
        current: b,
        change,
        difference: Some({ lower, upper, }),
      })
    }
    _ =>
      Some({
        baseline: a,
        current: b,
        change: MissingSamples,
        difference: None,
      })
  }
}
