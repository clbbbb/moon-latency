///|
pub(all) enum InputError {
  InputClosed
  ChunkTooLarge
  LineTooLong(Int64)
  InvalidRow(Int64)
  InvalidReading(Int64, ProducerError)
}

///|
/// Incremental reader for integer timestamp,latency[,weight] rows.
pub struct SampleInput {
  priv producer : Producer
  priv mut pending : String
  priv mut line : Int64
  priv mut closed : Bool
}

///|
pub fn SampleInput::new(producer : Producer) -> SampleInput {
  { producer, pending: "", line: 1L, closed: false, }
}

///|
const MAX_SAMPLE_LINE : Int32 = 96

///|
fn parse_sample(line : String) -> (Int64, Int64, Int64)? {
  for ch in line {
    if ch > '\u007f' {
      return None
    }
  }
  let fields = line.trim().split(",").to_array()
  if fields.length() < 2 || fields.length() > 3 {
    return None
  }
  Some(
    (
      @string.parse_int64(fields[0].trim()),
      @string.parse_int64(fields[1].trim()),
      if fields.length() == 3 {
        @string.parse_int64(fields[2].trim())
      } else {
        1L
      },
    ),
  ) catch {
    _ => None
  }
}

///|
fn SampleInput::parse_line(
  self : SampleInput,
  row : String,
) -> Result[Shard?, InputError] {
  if row.length() > MAX_SAMPLE_LINE {
    return Err(LineTooLong(self.line))
  }
  let (time, value, weight) = match parse_sample(row) {
    Some(sample) => sample
    None => return Err(InvalidRow(self.line))
  }
  match self.producer.record(time, value, count=weight) {
    Err(error) => Err(InvalidReading(self.line, error))
    Ok(shard) => {
      self.line += 1L
      Ok(shard)
    }
  }
}

///|
/// Completed shards are emitted before a later bad row is reported.
/// The callback must not call feed or finish on the same input.
pub fn SampleInput::feed(
  self : SampleInput,
  chunk : String,
  emit : (Shard) -> Unit,
) -> Result[Unit, InputError] {
  if self.closed {
    return Err(InputClosed)
  }
  if chunk.length() > 65536 {
    return Err(ChunkTooLarge)
  }
  let text = self.pending + chunk
  let mut start = 0
  for i in 0..<text.length() {
    if text[i] == '\n' {
      match self.parse_line(text[start:i].to_owned()) {
        Err(error) => {
          self.closed = true
          return Err(error)
        }
        Ok(Some(shard)) => emit(shard)
        Ok(None) => ()
      }
      start = i + 1
    }
  }
  self.pending = text[start:].to_owned()
  if self.pending.length() > MAX_SAMPLE_LINE {
    self.closed = true
    return Err(LineTooLong(self.line))
  }
  Ok(())
}

///|
pub fn SampleInput::finish(
  self : SampleInput,
) -> Result[Array[Shard], InputError] {
  if self.closed {
    return Err(InputClosed)
  }
  self.closed = true
  let items = []
  if !self.pending.is_empty() {
    match self.parse_line(self.pending) {
      Err(error) => return Err(error)
      Ok(Some(shard)) => items.push(shard)
      Ok(None) => ()
    }
  }
  if self.producer.finish() is Some(shard) {
    items.push(shard)
  }
  Ok(items)
}
