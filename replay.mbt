///|
pub(all) enum ReplayError {
  BadReadSize
  ReadFailure(String)
  JournalFailure(JournalError)
}

///|
pub(all) struct ReplayStats {
  frames : Int64
  added : Int64
  duplicates : Int64
  bytes : Int64
}

///|
/// Pull one chunk at a time from the host. The callback returns None at EOF.
/// A nonempty chunk is required so each successful read makes progress.
pub fn replay_journal(
  collector : Collector,
  read : () -> Result[Bytes?, String],
) -> Result[ReplayStats, ReplayError] {
  let reader = JournalReader::new(collector)
  let mut added = 0L
  let mut duplicates = 0L
  let mut bytes = 0L
  while true {
    let data = match read() {
      Ok(Some(chunk)) => chunk
      Ok(None) => break
      Err(error) => return Err(ReadFailure(error))
    }
    let length = data.length().to_int64()
    if length == 0L ||
      length > JOURNAL_CHUNK.to_int64() ||
      length > MAX_COUNT - bytes {
      return Err(BadReadSize)
    }
    bytes += length
    match
      reader.feed(data[:], receipt => {
        added += receipt.added.to_int64()
        duplicates += receipt.duplicates.to_int64()
      }) {
      Ok(_) => ()
      Err(error) => return Err(JournalFailure(error))
    }
  }
  match reader.finish() {
    Ok(_) => Ok({ frames: reader.frames(), added, duplicates, bytes, })
    Err(error) => Err(JournalFailure(error))
  }
}
