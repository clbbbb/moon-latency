///|
fn change_byte(data : Bytes, offset : Int32, value : Int32) -> Bytes {
  Bytes::from_array(
    Array::makei(data.length(), i => {
      if i == offset {
        value.to_byte()
      } else {
        data[i]
      }
    }),
  )
}

///|
test "checkpoint metadata cannot reopen expired or future epoch snapshots" {
  let collector = Collector::new(Milliseconds, 1L, 2, identities=1).unwrap()
  let item = packet(1L, 1L, 10L, Milliseconds, 400L, 5L)
  assert_true(collector.receive(item) is Ok(Added))
  let checkpoint = collector.checkpoint()
  assert_true(
    Collector::restore(change_byte(checkpoint, 20, 9))
    is Err(BadCheckpointFrame(0)),
  )
  assert_true(
    Collector::restore(change_byte(checkpoint, 20, 12))
    is Err(BadCheckpointState(0, WindowFailure(ClockWentBackward))),
  )
  assert_true(
    Collector::restore(change_byte(checkpoint, 22, 0))
    is Err(BadCheckpointConfiguration),
  )
  assert_true(
    Collector::restore(change_byte(checkpoint, 24, 0))
    is Err(BadCheckpointConfiguration),
  )
  assert_true(
    Collector::restore(change_byte(checkpoint, 26, 2))
    is Err(BadCheckpointConfiguration),
  )
  assert_true(
    Collector::restore(change_byte(checkpoint, 4, 7))
    is Err(BadCheckpointConfiguration),
  )
  assert_eq(collector.summary().count(), 5L)
}

///|
test "malformed transport values cannot bypass size order and signed counts" {
  let hist = Histogram::new()
  assert_true(hist.record(20L, count=MAX_COUNT - 2L))
  assert_true(hist.record(21L, count=2L))
  let bytes = hist.snapshot()
  assert_true(
    Histogram::from_snapshot(change_byte(bytes, 6, 128)) is Err(InvalidCount),
  )
  assert_true(
    Histogram::from_snapshot(change_byte(bytes, 23, 3)) is Err(InvalidCount),
  )
  assert_true(
    Histogram::from_snapshot(change_byte(bytes, 15, 20))
    is Err(InvalidBucketOrder),
  )
  let shard = Shard::new(3L, MAX_COUNT, MAX_COUNT, Nanoseconds, hist)
    .unwrap()
    .encode()
  assert_true(Shard::decode(change_byte(shard, 4, 255)) is Err(InvalidMetadata))
  for offset in [5, 13, 21] {
    assert_true(
      Shard::decode(change_byte(shard, offset, 128)) is Err(InvalidMetadata),
    )
  }
  assert_eq(Shard::decode(shard).unwrap().encode(), shard)
}

///|
test "registry checkpoint names require ASCII and canonical ordering" {
  let registry = Registry::new(capacity=2).unwrap()
  registry
  .register({
    service: "b",
    unit: Milliseconds,
    width: 10L,
    slots: 2,
    identities: 1,
  })
  .unwrap()
  registry
  .register({
    service: "a",
    unit: Milliseconds,
    width: 10L,
    slots: 2,
    identities: 1,
  })
  .unwrap()
  let bytes = registry.checkpoint()
  assert_true(
    Registry::restore(change_byte(bytes, 9, 255)) is Err(RegistryName(0)),
  )
  assert_true(
    Registry::restore(change_byte(bytes, 9, 98)) is Err(RegistryName(1)),
  )
  assert_true(
    Registry::restore(change_byte(bytes, 8, 0)) is Err(RegistryName(0)),
  )
  assert_true(
    Registry::restore(change_byte(bytes, 10, 255)) is Err(RegistryFrame(0)),
  )
  assert_true(
    Registry::restore(change_byte(bytes, 5, 0)) is Err(RegistryHeader),
  )
}

///|
test "zero-length batches and empty journals preserve the current collector clock" {
  let collector = Collector::new(Microseconds, 5L, 4).unwrap()
  collector.advance(MAX_COUNT).unwrap()
  let before = collector.checkpoint()
  let encoded = encode_batch([]).unwrap()
  assert_eq(encoded, b"MLB1\x00\x00")
  assert_true(
    collector.receive_batch_bytes(encoded) is Ok({ added: 0, duplicates: 0, }),
  )
  let out = @buffer.Buffer::Buffer()
  out.write_bytesview(journal_header()[:])
  out.write_bytesview(journal_frame([]).unwrap()[:])
  let data = out.to_bytes()
  let replay = replay_journal(collector, read_segments(data, 1)).unwrap()
  assert_eq(replay.bytes, data.length().to_int64())
  assert_eq(replay.frames, 1L)
  assert_eq(replay.added, 0L)
  assert_eq(replay.duplicates, 0L)
  assert_eq(collector.checkpoint(), before)
  assert_true(collector.advance(0L) is Err(WindowFailure(ClockWentBackward)))
}
