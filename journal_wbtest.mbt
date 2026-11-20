///|
test "journal uses the standard CRC and accepts arbitrary boundaries" {
  assert_eq(crc32(b"123456789"[:]), 0xCBF43926U)
  let a = packet(1L, 0L, 1L, Nanoseconds, 10L, 32L)
  let b = packet(2L, 0L, 31L, Nanoseconds, 111L, 30L)
  let out = @buffer.Buffer::Buffer()
  out.write_bytes(journal_header()[:])
  out.write_bytes(journal_frame([a]).unwrap()[:])
  out.write_bytes(journal_frame([b, b]).unwrap()[:])
  let bytes = out.to_bytes()
  for step in [1, 2, 3, 4, 7, 17, 33, 127] {
    let collector = Collector::new(Nanoseconds, 10L, 4).unwrap()
    let reader = JournalReader::new(collector)
    let receipts = []
    for offset = 0; offset < bytes.length(); offset = offset + step {
      let end = if offset + step > bytes.length() {
        bytes.length()
      } else {
        offset + step
      }
      reader.feed(bytes[offset:end], item => receipts.push(item)).unwrap()
    }
    reader.finish().unwrap()
    assert_eq(reader.frames(), 2L)
    assert_eq(receipts[1].duplicates, 1)
    assert_eq(collector.summary().count(), 62L)
  }
}

///|
test "corrupt frame never applies and previously committed frame stays replayable" {
  let first = packet(2L, 1L, 1L, Microseconds, 15L, 11L)
  let second = packet(2L, 2L, 91L, Microseconds, 15L, 12L)
  let out = @buffer.Buffer::Buffer()
  out.write_bytes(journal_header()[:])
  out.write_bytes(journal_frame([first]).unwrap()[:])
  let candidate = journal_frame([second]).unwrap()
  out.write_bytes(candidate[:candidate.length() - 1])
  out.write_byte((candidate[candidate.length() - 1].to_int() ^ 1).to_byte())
  let collector = Collector::new(Microseconds, 10L, 2).unwrap()
  let reader = JournalReader::new(collector)
  let received = []
  assert_true(
    reader.feed(out.to_bytes()[:], receipt => received.push(receipt))
    is Err(BadFrameChecksum(1L)),
  )
  assert_eq(received.length(), 1)
  assert_eq(collector.summary().count(), 11L)
  assert_true(collector.receive(first) is Ok(Duplicate))
  assert_true(reader.finish() is Err(JournalClosed))
}

///|
test "truncated frames do not modify the collector" {
  let sample = packet(1L, 1L, 1L, Nanoseconds, 13L, 3L)
  let frame = journal_frame([sample]).unwrap()
  for n in 1..<frame.length() {
    let collector = Collector::new(Nanoseconds, 5L, 1).unwrap()
    let reader = JournalReader::new(collector)
    reader.feed(journal_header()[:], _ => ()).unwrap()
    reader.feed(frame[:n], _ => ()).unwrap()
    assert_true(reader.finish() is Err(UnexpectedEnd(0L)))
    assert_eq(collector.summary().count(), 0L)
  }
}

///|
test "journal callbacks cannot change parser state during frame delivery" {
  let collector = Collector::new(Nanoseconds, 10L, 2).unwrap()
  let reader = JournalReader::new(collector)
  let shard = packet(3L, 2L, 0L, Nanoseconds, 1L, 2L)
  let frame = journal_frame([shard]).unwrap()
  let reentry = []
  reader.feed(journal_header()[:], _ => ()).unwrap()
  reader
  .feed(frame[:], receipt => {
    reentry.push(receipt.added == 1)
    reentry.push(reader.finish() is Err(JournalClosed))
    reentry.push(
      reader.feed(frame[:], _ => abort("unexpected nested receipt"))
      is Err(JournalClosed),
    )
  })
  .unwrap()
  assert_eq(reentry, [true, true, true])
  let duplicates = []
  reader.feed(frame[:], receipt => duplicates.push(receipt.duplicates)).unwrap()
  assert_eq(duplicates, [1])
  reader.finish().unwrap()
  assert_eq(collector.summary().count(), 2L)
}
