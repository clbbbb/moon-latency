///|
pub(all) enum ProducerError {
  InvalidProducer
  InvalidSample
  SampleOutOfOrder
  SampleCountOverflow
  SequenceExhausted
  ProducerFinished
}

///|
pub struct Producer {
  priv source : Int64
  priv unit : LatencyUnit
  priv width : Int64
  priv mut next : Int64
  priv mut epoch : Int64
  priv mut last_time : Int64
  priv mut pending : Histogram
  priv mut finished : Bool
}

///|
pub fn Producer::new(
  source : Int64,
  unit : LatencyUnit,
  width : Int64,
  sequence~ : Int64 = 0L,
) -> Result[Producer, ProducerError] {
  if source < 0L || sequence < 0L || width <= 0L {
    return Err(InvalidProducer)
  }
  Ok({
    source,
    unit,
    width,
    next: sequence,
    epoch: -1L,
    last_time: -1L,
    pending: Histogram::new(),
    finished: false,
  })
}

///|
fn Producer::seal(self : Producer) -> Shard {
  {
    source: self.source,
    sequence: self.next,
    timestamp: self.epoch * self.width,
    unit: self.unit,
    payload: self.pending.snapshot(),
    width: self.width,
    decoded: None,
  }
}

///|
/// Record a sample. Moving to a new epoch returns the preceding shard.
pub fn Producer::record(
  self : Producer,
  timestamp : Int64,
  latency : Int64,
  count~ : Int64 = 1L,
) -> Result[Shard?, ProducerError] {
  if self.finished {
    return Err(ProducerFinished)
  }
  if timestamp < 0L || latency < 0L || count <= 0L {
    return Err(InvalidSample)
  }
  if timestamp < self.last_time {
    return Err(SampleOutOfOrder)
  }
  let epoch = timestamp / self.width
  let previous = if self.epoch >= 0L && self.epoch != epoch {
    if self.next == MAX_COUNT {
      return Err(SequenceExhausted)
    }
    let shard = self.seal()
    self.pending = Histogram::new()
    self.next += 1L
    Some(shard)
  } else {
    None
  }
  if !self.pending.record(latency, count~) {
    return Err(SampleCountOverflow)
  }
  self.epoch = epoch
  self.last_time = timestamp
  Ok(previous)
}

///|
pub fn Producer::finish(self : Producer) -> Shard? {
  if self.finished {
    return None
  }
  self.finished = true
  if self.epoch < 0L {
    None
  } else {
    Some(self.seal())
  }
}

///|
pub fn Producer::pending_count(self : Producer) -> Int64 {
  if self.finished {
    0L
  } else {
    self.pending.count()
  }
}
