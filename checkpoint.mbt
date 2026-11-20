///|
pub(all) enum CheckpointError {
  BadCheckpointHeader
  BadCheckpointConfiguration
  BadCheckpointFrame(Int32)
  BadCheckpointShard(Int32, ShardError)
  BadCheckpointState(Int32, CollectError)
}

///|
/// MLC1 stores the collector configuration, epoch cursor, and active shards.
/// The wire format is a snapshot of accepted reports, not a memory dump.
pub fn Collector::checkpoint(self : Collector) -> Bytes {
  let out = @buffer.Buffer::Buffer()
  out.write_bytes(b"MLC1"[:])
  out.write_byte(self.unit.tag())
  append_word(out, self.window.width)
  append_word(out, self.window.latest)
  out.write_uint16_be(self.window.slots.length().to_uint16())
  out.write_uint16_be(self.capacity.to_uint16())
  out.write_uint16_be(self.seen.length().to_uint16())
  let shards = []
  for _, shard in self.seen {
    shards.push(shard)
  }
  shards.sort_by((a, b) => {
    let source = a.source.compare(b.source)
    if source == 0 {
      a.sequence.compare(b.sequence)
    } else {
      source
    }
  })
  for shard in shards {
    let data = shard.encode()
    out.write_uint16_be(data.length().to_uint16())
    out.write_bytes(data[:])
  }
  out.to_bytes()
}

///|
pub fn Collector::restore(bytes : Bytes) -> Result[Collector, CheckpointError] {
  let length = bytes.length()
  if length < 27 ||
    length > 27 + 4096 * (2 + MAX_SHARD_BYTES) ||
    bytes[:4].to_owned() != b"MLC1" {
    return Err(BadCheckpointHeader)
  }
  let unit = match bytes[4].to_int() {
    0 => Nanoseconds
    1 => Microseconds
    2 => Milliseconds
    _ => return Err(BadCheckpointConfiguration)
  }
  let width = read_word(bytes, 5)
  let epoch = read_word(bytes, 13)
  let slots = bytes.unsafe_read_uint16_be(21).to_int()
  let capacity = bytes.unsafe_read_uint16_be(23).to_int()
  let count = bytes.unsafe_read_uint16_be(25).to_int()
  if width <= 0L ||
    epoch < -1L ||
    epoch > MAX_COUNT / width ||
    count > capacity ||
    (epoch == -1L && count != 0) {
    return Err(BadCheckpointConfiguration)
  }
  let result = match Collector::new(unit, width, slots, identities=capacity) {
    Ok(collector) => collector
    Err(_) => return Err(BadCheckpointConfiguration)
  }
  result.window.advance_epoch(epoch)
  let mut offset = 27
  for index in 0..<count {
    if length - offset < 2 {
      return Err(BadCheckpointFrame(index))
    }
    let size = bytes.unsafe_read_uint16_be(offset).to_int()
    offset += 2
    if size < 33 || size > MAX_SHARD_BYTES || size > length - offset {
      return Err(BadCheckpointFrame(index))
    }
    let shard = match Shard::decode(bytes[offset:offset + size].to_owned()) {
      Ok(shard) => shard
      Err(error) => return Err(BadCheckpointShard(index, error))
    }
    if shard.timestamp / width > epoch {
      return Err(BadCheckpointFrame(index))
    }
    match result.receive(shard) {
      Ok(Added) => ()
      Ok(Duplicate) =>
        return Err(BadCheckpointState(index, ConflictingIdentity))
      Err(error) => return Err(BadCheckpointState(index, error))
    }
    offset += size
  }
  if offset != length {
    return Err(BadCheckpointFrame(count))
  }
  Ok(result)
}
