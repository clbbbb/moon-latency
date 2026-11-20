///|
pub(all) enum LatencyUnit {
  Nanoseconds
  Microseconds
  Milliseconds
}

///|
pub(all) enum ShardError {
  InvalidMetadata
  InvalidEncoding
  InvalidHistogram(SnapshotError)
}

///|
/// Identity is the pair (source, sequence). Producers must not reuse it
/// while a previous report with that identity can still be retried.
pub struct Shard {
  priv source : Int64
  priv sequence : Int64
  priv timestamp : Int64
  priv unit : LatencyUnit
  priv payload : Bytes
}

///|
pub fn Shard::new(
  source : Int64,
  sequence : Int64,
  timestamp : Int64,
  unit : LatencyUnit,
  histogram : Histogram,
) -> Result[Shard, ShardError] {
  if source < 0L || sequence < 0L || timestamp < 0L {
    return Err(InvalidMetadata)
  }
  Ok({ source, sequence, timestamp, unit, payload: histogram.snapshot(), })
}

///|
pub fn Shard::identity(self : Shard) -> (Int64, Int64) {
  (self.source, self.sequence)
}

///|
pub fn Shard::timestamp(self : Shard) -> Int64 {
  self.timestamp
}

///|
pub fn Shard::unit(self : Shard) -> LatencyUnit {
  self.unit
}

///|
fn LatencyUnit::tag(self : LatencyUnit) -> Byte {
  match self {
    Nanoseconds => 0
    Microseconds => 1
    Milliseconds => 2
  }
}

///|
fn append_word(out : @buffer.Buffer, word : Int64) -> Unit {
  for shift in [56, 48, 40, 32, 24, 16, 8, 0] {
    out.write_byte(((word >> shift) & 255L).to_int().to_byte())
  }
}

///|
fn read_word(bytes : Bytes, offset : Int32) -> Int64 {
  let mut word = 0L
  for i in offset..<(offset + 8) {
    word = (word << 8) | bytes[i].to_int().to_int64()
  }
  word
}

///|
/// MLS1 envelope: magic, unit byte, three big-endian signed words,
/// then one canonical MLH1 snapshot.
pub fn Shard::encode(self : Shard) -> Bytes {
  let out = @buffer.Buffer::Buffer()
  out.write_bytes(b"MLS1"[:])
  out.write_byte(self.unit.tag())
  append_word(out, self.source)
  append_word(out, self.sequence)
  append_word(out, self.timestamp)
  out.write_bytes(self.payload[:])
  out.to_bytes()
}

///|
pub fn Shard::decode(bytes : Bytes) -> Result[Shard, ShardError] {
  if bytes.length() < 33 ||
    bytes.length() > 33 + bounds.length() * 10 ||
    bytes[:4].to_owned() != b"MLS1" {
    return Err(InvalidEncoding)
  }
  let unit = match bytes[4].to_int() {
    0 => Nanoseconds
    1 => Microseconds
    2 => Milliseconds
    _ => return Err(InvalidMetadata)
  }
  let source = read_word(bytes, 5)
  let sequence = read_word(bytes, 13)
  let timestamp = read_word(bytes, 21)
  if source < 0L || sequence < 0L || timestamp < 0L {
    return Err(InvalidMetadata)
  }
  let payload = bytes[29:].to_owned()
  match Histogram::from_snapshot(payload) {
    Err(error) => Err(InvalidHistogram(error))
    Ok(_) => Ok({ source, sequence, timestamp, unit, payload, })
  }
}
