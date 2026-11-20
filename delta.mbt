///|
pub(all) enum DeltaError {
  UnitChanged
  SourceChanged
  SequenceDidNotIncrease
  TimestampWentBackward
  BucketDecreased(Int32)
}

///|
/// Compute the increment between two cumulative snapshots of the same series.
/// The first decreasing bucket is returned so callers can detect resets.
pub fn Histogram::delta(
  self : Histogram,
  baseline : Histogram,
) -> Result[Histogram, DeltaError] {
  let difference = Histogram::new()
  for i in 0..<bounds.length() {
    let before = baseline.counts[i]
    let after = self.counts[i]
    if after < before {
      return Err(BucketDecreased(i))
    }
    difference.counts[i] = after - before
  }
  difference.total = self.total - baseline.total
  Ok(difference)
}

///|
/// Convert a cumulative report into an incremental shard for the collector.
/// The caller must align collection with window epochs if exact attribution
/// to those epochs is required.
pub fn Shard::delta(
  self : Shard,
  baseline : Shard,
) -> Result[Shard, DeltaError] {
  if self.source != baseline.source {
    return Err(SourceChanged)
  }
  if self.unit.tag() != baseline.unit.tag() {
    return Err(UnitChanged)
  }
  if self.sequence <= baseline.sequence {
    return Err(SequenceDidNotIncrease)
  }
  if self.timestamp < baseline.timestamp {
    return Err(TimestampWentBackward)
  }
  // Shards can only contain validated canonical histogram payloads.
  let current = Histogram::from_snapshot(self.payload).unwrap()
  let previous = Histogram::from_snapshot(baseline.payload).unwrap()
  current.delta(previous).map(delta => { ..self, payload: delta.snapshot(), })
}
