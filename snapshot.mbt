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
  out.write_bytesview(b"MLH1"[:])
  for index in 0..<bounds.length() {
    if self.counts[index] != 0L {
      out.write_uint16_be(index.to_uint16())
      append_word(out, self.counts[index])
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
    let count = read_word(bytes, offset + 2)
    if count <= 0L || count > MAX_COUNT - result.total {
      return Err(InvalidCount)
    }
    result.counts[index] = count
    result.total += count
    last = index
  }
  Ok(result)
}
