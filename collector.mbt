///|
pub(all) enum CollectError {
  WindowFailure(WindowError)
  ShardFailure(ShardError)
  UnitMismatch
  IncompatibleTimeBucket
  ConflictingIdentity
  IdentityBudget
}

///|
pub(all) enum Receipt {
  Added
  Duplicate
}

///|
pub struct Collector {
  priv mut window : Window
  priv unit : LatencyUnit
  priv capacity : Int32
  priv mut seen : Map[(Int64, Int64), Shard]
}

///|
pub fn Collector::new(
  unit : LatencyUnit,
  width : Int64,
  slots : Int32,
  identities~ : Int32 = 1024,
) -> Result[Collector, CollectError] {
  if identities < 1 || identities > 4096 {
    return Err(IdentityBudget)
  }
  match Window::new(width, slots) {
    Ok(window) => Ok({ window, unit, capacity: identities, seen: Map([]), })
    Err(error) => Err(WindowFailure(error))
  }
}

///|
fn Collector::expired(self : Collector, epoch : Int64) -> Array[(Int64, Int64)] {
  let result : Array[(Int64, Int64)] = []
  let oldest = epoch - self.window.slots.length().to_int64() + 1L
  if epoch > self.window.latest {
    for id, shard in self.seen {
      if shard.timestamp / self.window.width < oldest {
        result.push(id)
      }
    }
  }
  result
}

///|
pub fn Collector::receive(
  self : Collector,
  shard : Shard,
) -> Result[Receipt, CollectError] {
  if shard.unit.tag() != self.unit.tag() {
    return Err(UnitMismatch)
  }
  if shard.width > 0L {
    let remaining = MAX_COUNT - shard.timestamp
    let coverage = if shard.width - 1L > remaining {
      remaining
    } else {
      shard.width - 1L
    }
    if coverage >= self.window.width - shard.timestamp % self.window.width {
      return Err(IncompatibleTimeBucket)
    }
  }
  let id = shard.identity()
  if self.seen.get(id) is Some(previous) {
    if previous.timestamp == shard.timestamp &&
      previous.width == shard.width &&
      previous.payload == shard.payload {
      return Ok(Duplicate)
    }
    return Err(ConflictingIdentity)
  }
  let epoch = shard.timestamp / self.window.width
  if epoch <= self.window.latest - self.window.slots.length().to_int64() {
    return Err(WindowFailure(ClockWentBackward))
  }
  let expired = self.expired(epoch)
  if self.seen.length() - expired.length() >= self.capacity {
    return Err(IdentityBudget)
  }
  let histogram = match shard.decoded {
    Some(value) => value
    None =>
      match Histogram::from_snapshot(shard.payload) {
        Err(error) => return Err(ShardFailure(InvalidHistogram(error)))
        Ok(value) => value
      }
  }
  match self.window.merge_live_epoch(epoch, histogram) {
    Err(error) => return Err(WindowFailure(error))
    Ok(_) => ()
  }
  for old in expired {
    self.seen.remove(old)
  }
  self.seen[id] = { ..shard, decoded: None, }
  Ok(Added)
}

///|
pub fn Collector::receive_bytes(
  self : Collector,
  bytes : Bytes,
) -> Result[Receipt, CollectError] {
  match Shard::decode(bytes) {
    Err(error) => Err(ShardFailure(error))
    Ok(shard) => self.receive(shard)
  }
}

///|
pub fn Collector::advance(
  self : Collector,
  timestamp : Int64,
) -> Result[Unit, CollectError] {
  let expired = self.expired(timestamp / self.window.width)
  match self.window.advance(timestamp) {
    Err(error) => return Err(WindowFailure(error))
    Ok(_) => ()
  }
  for old in expired {
    self.seen.remove(old)
  }
  Ok(())
}

///|
pub fn Collector::summary(self : Collector) -> Histogram {
  self.window.summary()
}
