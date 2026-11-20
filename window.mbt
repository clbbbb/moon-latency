///|
pub(all) enum WindowError {
  InvalidConfiguration
  InvalidObservation
  ClockWentBackward
  CountOverflow
}

///|
priv struct Slot {
  mut epoch : Int64
  histogram : Histogram
}

///|
/// A rolling histogram over a fixed number of aligned time buckets.
pub struct Window {
  priv width : Int64
  priv slots : Array[Slot]
  priv aggregate : Histogram
  priv mut latest : Int64
}

///|
pub fn Window::new(width : Int64, slots : Int32) -> Result[Window, WindowError] {
  if width <= 0L || slots < 1 || slots > 1024 {
    return Err(InvalidConfiguration)
  }
  Ok({
    width,
    slots: Array::makei(slots, _ => { epoch: -1L, histogram: Histogram::new(), }),
    aggregate: Histogram::new(),
    latest: -1L,
  })
}

///|
fn Window::advance_epoch(self : Window, epoch : Int64) -> Unit {
  let span = self.slots.length().to_int64()
  let mut expired = self.latest - span + 1L
  if expired < 0L {
    expired = 0L
  }
  let stop = if epoch - span < self.latest { epoch - span } else { self.latest }
  while expired <= stop {
    let slot = self.slots[(expired % span).to_int()]
    if slot.epoch == expired {
      for i in 0..<bounds.length() {
        self.aggregate.counts[i] -= slot.histogram.counts[i]
        slot.histogram.counts[i] = 0L
      }
      self.aggregate.total -= slot.histogram.total
      slot.histogram.total = 0L
      slot.epoch = -1L
    }
    expired += 1L
  }
  self.latest = epoch
}

///|
pub fn Window::advance(
  self : Window,
  time : Int64,
) -> Result[Unit, WindowError] {
  if time < 0L {
    return Err(InvalidObservation)
  }
  let epoch = time / self.width
  if epoch < self.latest {
    return Err(ClockWentBackward)
  }
  self.advance_epoch(epoch)
  Ok(())
}

///|
/// Insert a shard summary. Timestamps identify aligned time buckets.
/// Rejection leaves both the contents and the time cursor unchanged.
pub fn Window::merge_at(
  self : Window,
  time : Int64,
  shard : Histogram,
) -> Result[Unit, WindowError] {
  if time < 0L {
    return Err(InvalidObservation)
  }
  let epoch = time / self.width
  if epoch < self.latest {
    return Err(ClockWentBackward)
  }
  self.merge_live_epoch(epoch, shard)
}

///|
// The collector may insert into an older, still-live slot.
fn Window::merge_live_epoch(
  self : Window,
  epoch : Int64,
  shard : Histogram,
) -> Result[Unit, WindowError] {
  let span = self.slots.length().to_int64()
  if epoch <= self.latest - span {
    return Err(ClockWentBackward)
  }
  let mut remaining = self.aggregate.total
  if shard.total > MAX_COUNT - remaining {
    for slot in self.slots {
      if slot.epoch >= 0L && slot.epoch <= epoch - span {
        remaining -= slot.histogram.total
      }
    }
    if shard.total > MAX_COUNT - remaining {
      return Err(CountOverflow)
    }
  }
  if epoch > self.latest {
    self.advance_epoch(epoch)
  }
  let slot = self.slots[(epoch % span).to_int()]
  slot.epoch = epoch
  ignore(slot.histogram.merge(shard))
  ignore(self.aggregate.merge(shard))
  Ok(())
}

///|
pub fn Window::summary(self : Window) -> Histogram {
  { counts: self.aggregate.counts.copy(), total: self.aggregate.total, }
}

///|
fn Window::copy(self : Window) -> Window {
  {
    width: self.width,
    slots: self.slots.map(slot => {
      epoch: slot.epoch,
      histogram: {
        counts: slot.histogram.counts.copy(),
        total: slot.histogram.total,
      },
    }),
    aggregate: self.summary(),
    latest: self.latest,
  }
}
