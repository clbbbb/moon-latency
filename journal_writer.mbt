///|
pub(all) enum WriterError {
  WriterClosed
  FrameRejected(BatchError)
  InvalidWriterLimit
  ShardExceedsFrameLimit
}

///|
/// Collect shards into bounded frames. An oversized next frame is split
/// before accepting the next shard, without dropping it.
pub struct JournalWriter {
  priv count_limit : Int32
  priv bytes_limit : Int32
  priv mut pending : Array[Shard]
  priv mut bytes : Int32
  priv mut closed : Bool
}

///|
pub fn JournalWriter::new(
  count_limit~ : Int32 = 64,
  bytes_limit~ : Int32 = 65536,
) -> Result[JournalWriter, WriterError] {
  if count_limit < 1 ||
    count_limit > BATCH_LIMIT ||
    bytes_limit < 6 + 2 + MAX_SHARD_BYTES - 8 ||
    bytes_limit > MAX_BATCH_BYTES {
    return Err(InvalidWriterLimit)
  }
  Ok({ count_limit, bytes_limit, pending: [], bytes: 6, closed: false, })
}

///|
fn JournalWriter::drain(self : JournalWriter) -> Result[Bytes?, WriterError] {
  if self.pending.is_empty() {
    return Ok(None)
  }
  match journal_frame(self.pending) {
    Err(error) => Err(FrameRejected(error))
    Ok(frame) => {
      self.pending = []
      self.bytes = 6
      Ok(Some(frame))
    }
  }
}

///|
pub fn JournalWriter::push(
  self : JournalWriter,
  shard : Shard,
) -> Result[Bytes?, WriterError] {
  if self.closed {
    return Err(WriterClosed)
  }
  let header = if shard.width == 0L { 29 } else { 37 }
  let record = 2 + header + shard.payload.length()
  if 6 + record > self.bytes_limit {
    return Err(ShardExceedsFrameLimit)
  }
  let emitted = if self.pending.length() == self.count_limit ||
    self.bytes + record > self.bytes_limit {
    match self.drain() {
      Ok(bytes) => bytes
      Err(error) => return Err(error)
    }
  } else {
    None
  }
  self.pending.push(shard)
  self.bytes += record
  Ok(emitted)
}

///|
pub fn JournalWriter::flush(
  self : JournalWriter,
) -> Result[Bytes?, WriterError] {
  if self.closed {
    return Err(WriterClosed)
  }
  self.drain()
}

///|
pub fn JournalWriter::finish(
  self : JournalWriter,
) -> Result[Bytes?, WriterError] {
  if self.closed {
    return Err(WriterClosed)
  }
  match self.drain() {
    Ok(frame) => {
      self.closed = true
      Ok(frame)
    }
    Err(error) => Err(error)
  }
}

///|
pub fn JournalWriter::pending_count(self : JournalWriter) -> Int32 {
  self.pending.length()
}
