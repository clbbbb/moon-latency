///|
fn append_optional(out : @buffer.Buffer, bytes : Bytes?) -> Unit {
  if bytes is Some(value) {
    out.write_bytes(value[:])
  }
}

///|
fn read_segments(bytes : Bytes, size : Int32) -> () -> Result[Bytes?, String] {
  let mut cursor = 0
  fn() {
    if cursor >= bytes.length() {
      return Ok(None)
    }
    let count = if bytes.length() - cursor < size {
      bytes.length() - cursor
    } else {
      size
    }
    let end = cursor + count
    let result = bytes[cursor:end].to_owned()
    cursor = end
    Ok(Some(result))
  }
}

///|
test "journal frame length budget can split dense shards without dropping any" {
  let hist = Histogram::new()
  for value in bounds {
    assert_true(hist.record(value))
  }
  let shard = Shard::new(1L, 1L, 0L, Milliseconds, hist).unwrap()
  let second = Shard::new(1L, 2L, 0L, Milliseconds, hist).unwrap()
  let writer = JournalWriter::new(count_limit=5, bytes_limit=9641).unwrap()
  let out = @buffer.Buffer::Buffer()
  out.write_bytes(journal_header()[:])
  assert_true(writer.push(shard) is Ok(None))
  assert_eq(writer.pending_count(), 1)
  let first = writer.push(second).unwrap().unwrap()
  assert_eq(first.length(), 9649)
  out.write_bytes(first[:])
  assert_eq(writer.pending_count(), 1)
  append_optional(out, writer.finish().unwrap())
  assert_true(writer.flush() is Err(WriterClosed))
  assert_true(writer.push(shard) is Err(WriterClosed))
  let collector = Collector::new(Milliseconds, 1L, 1).unwrap()
  let bytes = out.to_bytes()
  let stats = replay_journal(collector, read_segments(bytes, 4096)).unwrap()
  assert_eq(stats.frames, 2L)
  assert_eq(stats.added, 2L)
  assert_eq(stats.duplicates, 0L)
  assert_eq(stats.bytes, bytes.length().to_int64())
  assert_eq(collector.summary().count(), 1920L)
  assert_eq(collector.summary().count_at_most(31L).lower, 64L)
}

///|
test "journal count budget and explicit flush delimit repeatable frames" {
  assert_true(JournalWriter::new(count_limit=0) is Err(InvalidWriterLimit))
  assert_true(JournalWriter::new(bytes_limit=9640) is Err(InvalidWriterLimit))
  let writer = JournalWriter::new(count_limit=2).unwrap()
  assert_true(writer.flush() is Ok(None))
  let a = packet(1L, 1L, 0L, Microseconds, 6L, 17L)
  let b = packet(2L, 1L, 0L, Microseconds, 18L, 7L)
  let out = @buffer.Buffer::Buffer()
  out.write_bytes(journal_header()[:])
  assert_true(writer.push(a) is Ok(None))
  assert_true(writer.push(b) is Ok(None))
  append_optional(out, writer.push(a).unwrap())
  assert_eq(writer.pending_count(), 1)
  append_optional(out, writer.flush().unwrap())
  assert_eq(writer.pending_count(), 0)
  assert_true(writer.finish() is Ok(None))
  let collector = Collector::new(Microseconds, 1L, 1).unwrap()
  let stats = replay_journal(collector, read_segments(out.to_bytes(), 2)).unwrap()
  assert_eq(stats.frames, 2L)
  assert_eq(stats.added, 2L)
  assert_eq(stats.duplicates, 1L)
  assert_eq(collector.summary().count(), 24L)
}

///|
test "host read errors and incomplete final batches preserve completed frames" {
  let collector = Collector::new(Nanoseconds, 10L, 2).unwrap()
  let item = packet(2L, 1L, 0L, Nanoseconds, 3L, 1L)
  let complete = journal_frame([item]).unwrap()
  let mut reads = 0
  let result = replay_journal(collector, () => {
    reads += 1
    match reads {
      1 => Ok(Some(journal_header()))
      2 => Ok(Some(complete))
      _ => Err("I/O failure")
    }
  })
  assert_true(result is Err(ReadFailure("I/O failure")))
  assert_eq(collector.summary().count(), 1L)
  assert_true(
    replay_journal(collector, () => Ok(Some(b""))) is Err(BadReadSize),
  )
  assert_true(
    replay_journal(collector, () => Ok(None))
    is Err(JournalFailure(UnexpectedEnd(0L))),
  )
  assert_true(collector.receive(item) is Ok(Duplicate))
}

///|
test "valid checksums do not bypass journal semantic validation" {
  let collector = Collector::new(Milliseconds, 1L, 1).unwrap()
  let valid = packet(1L, 1L, 0L, Milliseconds, 0L, 4L)
  let invalid = packet(7L, 7L, 0L, Microseconds, 0L, 2L)
  let reader = JournalReader::new(collector)
  reader.feed(journal_header()[:], _ => ()).unwrap()
  let frame = journal_frame([valid, invalid]).unwrap()
  assert_true(
    reader.feed(frame[:], _ => ())
    is Err(BadJournalBatch(0L, RejectedShard(1, UnitMismatch))),
  )
  assert_eq(collector.summary().count(), 0L)
  assert_true(collector.receive(valid) is Ok(Added))
}
