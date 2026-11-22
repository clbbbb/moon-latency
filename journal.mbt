///|
pub(all) enum JournalError {
  JournalClosed
  BadJournalMagic
  BadFrameLength(Int64)
  BadFrameChecksum(Int64)
  UnexpectedEnd(Int64)
  BadJournalBatch(Int64, BatchCollectError)
  OversizedChunk
}

///|
/// Incremental MLJ1 reader. Completed batches are applied atomically.
/// A bad frame seals the reader and preserves the accepted frame prefix.
pub struct JournalReader {
  priv collector : Collector
  priv mut pending : Array[Byte]
  priv mut required : Int32
  priv mut phase : JournalPhase
  priv mut frame : Int64
  priv mut closed : Bool
  priv mut delivering : Bool
}

///|
priv enum JournalPhase {
  Magic
  Length
  Body
}

///|
const MAX_BATCH_BYTES : Int32 = 9874438

///|
const JOURNAL_CHUNK : Int32 = 65536

///|
let crc_table : Array[UInt32] = Array::makei(256, i => {
  let mut state = i.reinterpret_as_uint()
  for _ in 0..<8 {
    state = if (state & 1U) == 0U {
      state >> 1
    } else {
      (state >> 1) ^ 0xEDB88320U
    }
  }
  state
})

///|
fn crc32(data : BytesView) -> UInt32 {
  let mut state = 0xFFFFFFFFU
  for byte in data {
    let i = ((state ^ byte.to_int().reinterpret_as_uint()) & 255U).reinterpret_as_int()
    state = (state >> 8) ^ crc_table[i]
  }
  state ^ 0xFFFFFFFFU
}

///|
pub fn journal_header() -> Bytes {
  b"MLJ1"
}

///|
fn append_dword(out : @buffer.Buffer, word : UInt32) -> Unit {
  out.write_uint16_be((word >> 16).reinterpret_as_int().to_uint16())
  out.write_uint16_be((word & 65535U).reinterpret_as_int().to_uint16())
}

///|
/// Append a single frame following the MLJ1 header (or another frame).
pub fn journal_frame(shards : Array[Shard]) -> Result[Bytes, BatchError] {
  let batch = match encode_batch(shards) {
    Ok(bytes) => bytes
    Err(error) => return Err(error)
  }
  let out = @buffer.Buffer::Buffer()
  append_dword(out, batch.length().reinterpret_as_uint())
  out.write_bytes(batch[:])
  append_dword(out, crc32(batch[:]))
  Ok(out.to_bytes())
}

///|
pub fn JournalReader::new(collector : Collector) -> JournalReader {
  {
    collector,
    pending: Array(capacity=4),
    required: 4,
    phase: Magic,
    frame: 0L,
    closed: false,
    delivering: false,
  }
}

///|
pub fn JournalReader::frames(self : JournalReader) -> Int64 {
  self.frame
}

///|
fn JournalReader::next_frame(
  self : JournalReader,
  emit : (BatchReceipt) -> Unit,
) -> Result[Unit, JournalError] {
  let data = Bytes::from_array(self.pending)
  match self.phase {
    Magic => {
      if data != journal_header() {
        return Err(BadJournalMagic)
      }
      self.phase = Length
    }
    Length => {
      let length = data.unsafe_read_uint32_be(0)
      if length < 6U || length > MAX_BATCH_BYTES.reinterpret_as_uint() {
        return Err(BadFrameLength(self.frame))
      }
      self.required = length.reinterpret_as_int() + 4
      self.phase = Body
    }
    Body => {
      let checksum_offset = data.length() - 4
      let expected = data.unsafe_read_uint32_be(checksum_offset)
      if expected != crc32(data[:checksum_offset]) {
        return Err(BadFrameChecksum(self.frame))
      }
      let receipt = match
        self.collector.receive_batch_bytes(data[:checksum_offset].to_owned()) {
        Err(error) => return Err(BadJournalBatch(self.frame, error))
        Ok(receipt) => receipt
      }
      self.frame += 1L
      self.required = 4
      self.phase = Length
      self.delivering = true
      emit(receipt)
      self.delivering = false
    }
  }
  self.pending = Array(capacity=4)
  Ok(())
}

///|
/// Reentrant reads and closes are rejected during callback delivery.
pub fn JournalReader::feed(
  self : JournalReader,
  chunk : BytesView,
  emit : (BatchReceipt) -> Unit,
) -> Result[Unit, JournalError] {
  if self.closed || self.delivering {
    return Err(JournalClosed)
  }
  if chunk.length() > JOURNAL_CHUNK {
    return Err(OversizedChunk)
  }
  let mut offset = 0
  while offset < chunk.length() {
    let available = chunk.length() - offset
    let needed = self.required - self.pending.length()
    let take = if available < needed { available } else { needed }
    for i in offset..<(offset + take) {
      self.pending.push(chunk[i])
    }
    offset += take
    if self.pending.length() == self.required {
      match self.next_frame(emit) {
        Err(error) => {
          self.pending = []
          self.closed = true
          return Err(error)
        }
        Ok(_) => ()
      }
    }
  }
  Ok(())
}

///|
pub fn JournalReader::finish(
  self : JournalReader,
) -> Result[Unit, JournalError] {
  if self.closed || self.delivering {
    return Err(JournalClosed)
  }
  let complete = self.phase is Length && self.pending.is_empty()
  self.closed = true
  self.pending = []
  if complete {
    Ok(())
  } else {
    Err(UnexpectedEnd(self.frame))
  }
}
