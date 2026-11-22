///|
pub(all) enum BatchError {
  InvalidBatchSize
  InvalidBatchHeader
  InvalidBatchFrame(Int32)
  InvalidBatchShard(Int32, ShardError)
}

///|
const BATCH_LIMIT : Int32 = 1024

///|
const MAX_SHARD_BYTES : Int32 = 9641

///|
/// Portable batch with a fixed header, a count, and length-prefixed shards.
pub fn encode_batch(shards : Array[Shard]) -> Result[Bytes, BatchError] {
  if shards.length() > BATCH_LIMIT {
    return Err(InvalidBatchSize)
  }
  let out = @buffer.Buffer::Buffer()
  out.write_bytes(b"MLB1"[:])
  out.write_uint16_be(shards.length().to_uint16())
  for shard in shards {
    let bytes = shard.encode()
    out.write_uint16_be(bytes.length().to_uint16())
    out.write_bytes(bytes[:])
  }
  Ok(out.to_bytes())
}

///|
pub fn decode_batch(bytes : Bytes) -> Result[Array[Shard], BatchError] {
  let length = bytes.length()
  if length < 6 || length > 6 + BATCH_LIMIT * (2 + MAX_SHARD_BYTES) {
    return Err(InvalidBatchSize)
  }
  if bytes[:4].to_owned() != b"MLB1" {
    return Err(InvalidBatchHeader)
  }
  let count = bytes.unsafe_read_uint16_be(4).to_int()
  if count > BATCH_LIMIT {
    return Err(InvalidBatchSize)
  }
  let shards : Array[Shard] = []
  let mut offset = 6
  for index in 0..<count {
    if length - offset < 2 {
      return Err(InvalidBatchFrame(index))
    }
    let size = bytes.unsafe_read_uint16_be(offset).to_int()
    offset += 2
    if size < 33 || size > MAX_SHARD_BYTES || size > length - offset {
      return Err(InvalidBatchFrame(index))
    }
    let shard = match Shard::decode(bytes[offset:offset + size].to_owned()) {
      Ok(shard) => shard
      Err(error) => return Err(InvalidBatchShard(index, error))
    }
    shards.push(shard)
    offset += size
  }
  if offset != length {
    return Err(InvalidBatchFrame(count))
  }
  Ok(shards)
}

///|
pub(all) enum BatchCollectError {
  MalformedBatch(BatchError)
  BatchTooLarge
  RejectedShard(Int32, CollectError)
}

///|
pub(all) struct BatchReceipt {
  added : Int32
  duplicates : Int32
}

///|
fn Collector::copy(self : Collector) -> Collector {
  let seen = Map([])
  for key, value in self.seen {
    seen[key] = value
  }
  {
    unit: self.unit,
    capacity: self.capacity,
    seen,
    window: self.window.copy(),
  }
}

///|
/// Process in input order and apply all changes only after success.
pub fn Collector::receive_batch(
  self : Collector,
  shards : Array[Shard],
) -> Result[BatchReceipt, BatchCollectError] {
  if shards.length() > BATCH_LIMIT {
    return Err(BatchTooLarge)
  }
  if shards.is_empty() {
    return Ok({ added: 0, duplicates: 0, })
  }
  let staged = self.copy()
  let mut added = 0
  let mut duplicates = 0
  for i in 0..<shards.length() {
    match staged.receive(shards[i]) {
      Ok(Added) => added += 1
      Ok(Duplicate) => duplicates += 1
      Err(error) => return Err(RejectedShard(i, error))
    }
  }
  self.window = staged.window
  self.seen = staged.seen
  Ok({ added, duplicates, })
}

///|
pub fn Collector::receive_batch_bytes(
  self : Collector,
  bytes : Bytes,
) -> Result[BatchReceipt, BatchCollectError] {
  match decode_batch(bytes) {
    Ok(shards) => self.receive_batch(shards)
    Err(error) => Err(MalformedBatch(error))
  }
}
