///|
test "cross-epoch batch failure leaves previous state and retry tracking unchanged" {
  let initial = packet(10L, 1L, 0L, Milliseconds, 33L, 31L)
  let fresh = packet(10L, 2L, 90L, Milliseconds, 400L, 12L)
  let invalid = packet(20L, 1L, 90L, Microseconds, 49L, 32L)
  let collector = Collector::new(Milliseconds, 10L, 3, identities=1).unwrap()
  assert_true(collector.receive(initial) is Ok(Added))
  let before = collector.checkpoint()
  let batch = encode_batch([fresh, invalid]).unwrap()
  assert_true(
    collector.receive_batch_bytes(batch) is Err(RejectedShard(1, UnitMismatch)),
  )
  assert_eq(collector.checkpoint(), before)
  assert_true(collector.receive(initial) is Ok(Duplicate))
  let retry = encode_batch([fresh, fresh]).unwrap()
  let receipt = collector.receive_batch_bytes(retry).unwrap()
  assert_eq(receipt.added, 1)
  assert_eq(receipt.duplicates, 1)
  assert_eq(collector.summary().count(), 12L)
}

///|
test "checkpoint roundtrip preserves empty clocks and active retries" {
  let collector = Collector::new(Nanoseconds, 10L, 7).unwrap()
  let initial = collector.checkpoint()
  assert_eq(Collector::restore(initial).unwrap().checkpoint(), initial)
  collector.advance(700L).unwrap()
  let idle = Collector::restore(collector.checkpoint()).unwrap()
  assert_true(idle.advance(690L) is Err(WindowFailure(ClockWentBackward)))
  let a = packet(200L, 10L, 671L, Nanoseconds, 1L, 1000L)
  let b = packet(1L, 1L, 702L, Nanoseconds, 918L, 2000L)
  assert_true(collector.receive(a) is Ok(Added))
  assert_true(collector.receive(b) is Ok(Added))
  let checkpoint = collector.checkpoint()
  let restored = Collector::restore(checkpoint).unwrap()
  assert_eq(restored.checkpoint(), checkpoint)
  assert_eq(restored.summary().snapshot(), collector.summary().snapshot())
  assert_true(restored.receive(a) is Ok(Duplicate))
  restored.advance(740L).unwrap()
  assert_true(restored.receive(a) is Err(WindowFailure(ClockWentBackward)))
  assert_eq(restored.summary().count(), 2000L)
  assert_eq(collector.summary().count(), 3000L)
}

///|
test "all strict prefixes of batches and checkpoints are rejected" {
  let samples = [
    packet(0L, 0L, 0L, Microseconds, 33L, 1L),
    packet(1L, 0L, 0L, Microseconds, MAX_COUNT, 2L),
  ]
  let batch = encode_batch(samples).unwrap()
  for i in 0..<batch.length() {
    assert_true(decode_batch(batch[:i].to_owned()) is Err(_))
  }
  let collector = Collector::new(Microseconds, 1L, 1).unwrap()
  assert_true(
    collector.receive_batch_bytes(batch) is Ok({ added: 2, duplicates: 0, }),
  )
  let checkpoint = collector.checkpoint()
  for i in 0..<checkpoint.length() {
    assert_true(Collector::restore(checkpoint[:i].to_owned()) is Err(_))
  }
}

///|
test "every text split preserves producer sequencing and weighted counts" {
  let source = "0,10,4\r\n2,20\n10,1000,3\n34,500,20"
  for split in 0..<(source.length() + 1) {
    let producer = Producer::new(22L, Microseconds, 10L, sequence=19L).unwrap()
    let input = SampleInput::new(producer)
    let output = []
    input.feed(source[:split].to_owned(), shard => output.push(shard)).unwrap()
    input.feed(source[split:].to_owned(), shard => output.push(shard)).unwrap()
    for shard in input.finish().unwrap() {
      output.push(shard)
    }
    assert_eq(output.length(), 3)
    assert_eq(output[0].identity(), (22L, 19L))
    assert_eq(output[1].timestamp(), 10L)
    assert_eq(output[2].timestamp(), 30L)
    let collector = Collector::new(Microseconds, 10L, 6).unwrap()
    collector.receive_batch_bytes(encode_batch(output).unwrap()).unwrap()
    |> ignore
    assert_eq(collector.summary().count(), 28L)
    assert_eq(collector.summary().count_at_most(31L).lower, 5L)
  }
}

///|
test "bad rows preserve emitted prefix and identify the physical record" {
  let input = SampleInput::new(Producer::new(11L, Nanoseconds, 5L).unwrap())
  let emitted = []
  assert_true(
    input.feed("0,7\n5,8\n4,9\n", shard => emitted.push(shard))
    is Err(InvalidReading(3L, SampleOutOfOrder)),
  )
  assert_eq(emitted.length(), 1)
  assert_eq(emitted[0].timestamp(), 0L)
  assert_true(input.finish() is Err(InputClosed))
}

///|
test "sequence and timestamp limits reject before changing the pending sample" {
  let producer = Producer::new(40L, Milliseconds, 7L, sequence=MAX_COUNT).unwrap()
  assert_true(producer.record(0L, 30L, count=MAX_COUNT) is Ok(None))
  assert_true(producer.record(1L, 31L) is Err(SampleCountOverflow))
  assert_true(producer.record(7L, 32L) is Err(SequenceExhausted))
  assert_eq(producer.pending_count(), MAX_COUNT)
  let shard = producer.finish().unwrap()
  assert_eq(shard.identity(), (40L, MAX_COUNT))
  assert_eq(Histogram::from_snapshot(shard.payload).unwrap().count(), MAX_COUNT)
  assert_true(producer.finish() is None)
}

///|
test "series includes empty epochs and survives the largest timestamp" {
  let collector = Collector::new(Milliseconds, 1L, 3).unwrap()
  let edge = MAX_COUNT - 2L
  assert_true(
    collector.receive(packet(3L, 1L, edge, Milliseconds, 20L, 10L)) is Ok(Added),
  )
  assert_true(
    collector.receive(packet(3L, 2L, MAX_COUNT, Milliseconds, 410L, 10L))
    is Ok(Added),
  )
  let reports = collector.series(400L, 990000).unwrap()
  assert_eq(reports.length(), 3)
  assert_eq(reports[0].started_at, edge)
  assert_true(reports[0].result.status is Met)
  assert_true(reports[1].result.status is NoSamples)
  assert_true(reports[2].result.status is Indeterminate)
  assert_eq(reports[2].started_at, MAX_COUNT)
}

///|
test "quantile comparison reports rigorous signed changes" {
  let left = Histogram::new()
  let right = Histogram::new()
  assert_true(
    compare_quantile(left.distribution(), right.distribution(), 990000)
    is Some({ change: MissingSamples, .. }),
  )
  assert_true(left.record(1L))
  assert_true(right.record(400L))
  let a = left.distribution()
  let b = right.distribution()
  assert_true(
    compare_quantile(a, b, 990000)
    is Some(
      { change: Slower, difference: Some({ lower: 399L, upper: 414L, }), .. }
    ),
  )
  assert_true(
    compare_quantile(b, a, 990000)
    is Some(
      { change: Faster, difference: Some({ lower: -414L, upper: -399L, }), .. }
    ),
  )
  assert_true(compare_quantile(b, b, 990000) is Some({ change: Uncertain, .. }))
  assert_true(compare_quantile(a, a, 990000) is Some({ change: ExactTie, .. }))
}
