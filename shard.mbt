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
  priv width : Int64
  priv decoded : Histogram?
}

///|
pub fn Shard::new(
  source : Int64,
  sequence : Int64,
  timestamp : Int64,
  unit : LatencyUnit,
  histogram : Histogram,
  width~ : Int64 = 0L,
) -> Result[Shard, ShardError] {
  if source < 0L ||
    sequence < 0L ||
    timestamp < 0L ||
    !valid_shard_width(timestamp, width) {
    return Err(InvalidMetadata)
  }
  Ok({
    source,
    sequence,
    timestamp,
    unit,
    payload: histogram.snapshot(),
    width,
    decoded: None,
  })
}

///|
fn valid_shard_width(timestamp : Int64, width : Int64) -> Bool {
  width >= 0L && (width == 0L || timestamp % width == 0L)
}

///|
/// Zero denotes point data or an MLS1 shard without coverage metadata.
pub fn Shard::bucket_width(self : Shard) -> Int64 {
  self.width
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
/// MLS2 adds an aligned bucket width after the MLS1 timestamp.
pub fn Shard::encode(self : Shard) -> Bytes {
  let out = @buffer.Buffer::Buffer()
  let magic = if self.width == 0L { b"MLS1" } else { b"MLS2" }
  out.write_bytesview(magic[:])
  out.write_byte(self.unit.tag())
  append_word(out, self.source)
  append_word(out, self.sequence)
  append_word(out, self.timestamp)
  if self.width > 0L {
    append_word(out, self.width)
  }
  out.write_bytesview(self.payload[:])
  out.to_bytes()
}

///|
pub fn Shard::decode(bytes : Bytes) -> Result[Shard, ShardError] {
  if bytes.length() < 33 || bytes.length() > MAX_SHARD_BYTES {
    return Err(InvalidEncoding)
  }
  let header = match bytes[:4].to_owned() {
    b"MLS1" => 29
    b"MLS2" if bytes.length() >= 41 => 37
    _ => return Err(InvalidEncoding)
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
  let width = if header == 37 { read_word(bytes, 29) } else { 0L }
  if source < 0L ||
    sequence < 0L ||
    timestamp < 0L ||
    (header == 37 && width == 0L) ||
    !valid_shard_width(timestamp, width) {
    return Err(InvalidMetadata)
  }
  let payload = bytes[header:].to_owned()
  match Histogram::from_snapshot(payload) {
    Err(error) => Err(InvalidHistogram(error))
    Ok(histogram) =>
      Ok({
        source,
        sequence,
        timestamp,
        unit,
        payload,
        width,
        decoded: Some(histogram),
      })
  }
}
