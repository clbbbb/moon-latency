///|
pub(all) enum SnapshotError {
  InvalidSize
  InvalidHeader
  InvalidBucketOrder
  InvalidCount
}

///|
// MLH1 uses a sparse list of (u16 bucket index, i64 positive count) in
// big-endian order. The payload length determines the number of records.
pub fn Histogram::snapshot(self : Histogram) -> Bytes {
  let out = @buffer.Buffer::Buffer()
  for byte in b"MLH1" {
    out.write_byte(byte)
  }
  for index in 0..<bounds.length() {
    if self.counts[index] != 0L {
      out.write_uint16_be(index.to_uint16())
      for shift in [56, 48, 40, 32, 24, 16, 8, 0] {
        out.write_byte(
          ((self.counts[index] >> shift) & 255L).to_int().to_byte(),
        )
      }
    }
  }
  out.to_bytes()
}

///|
pub fn Histogram::from_snapshot(
  bytes : Bytes,
) -> Result[Histogram, SnapshotError] {
  let length = bytes.length()
  if length < 4 || length > 4 + bounds.length() * 10 || (length - 4) % 10 != 0 {
    return Err(InvalidSize)
  }
  if bytes[:4].to_owned() != b"MLH1" {
    return Err(InvalidHeader)
  }
  let result = Histogram::new()
  let mut last = -1
  for offset = 4; offset < length; offset = offset + 10 {
    let index = bytes.unsafe_read_uint16_be(offset).to_int()
    if index <= last || index >= bounds.length() {
      return Err(InvalidBucketOrder)
    }
    if bytes[offset + 2].to_int() >= 128 {
      return Err(InvalidCount)
    }
    let mut count = 0L
    for i in (offset + 2)..<(offset + 10) {
      count = (count << 8) | bytes[i].to_int().to_int64()
    }
    if count == 0L || !result.record(bounds[index], count~) {
      return Err(InvalidCount)
    }
    last = index
  }
  Ok(result)
}
