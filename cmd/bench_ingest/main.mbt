///|
fn run(label : String, buckets : Int32, batch_size : Int32) -> Unit {
  let histogram = @latency.Histogram::new()
  for i in 0..<buckets {
    // Spread samples across consecutive powers of two and sub-buckets.
    let base = 32L << (i / 16)
    ignore(histogram.record(base + base / 16L * (i % 16).to_int64()))
  }
  let samples = Array::makei(192, i => {
    @latency.Shard::new(
      0L,
      i.to_int64(),
      (i / 8).to_int64(),
      @latency.Milliseconds,
      histogram,
    ).unwrap()
  })
  let packets = samples.map(sample => sample.encode())
  let batches = Array::makei(192 / batch_size, i => {
    @latency.encode_batch(
      samples[i * batch_size:(i + 1) * batch_size].to_owned(),
    ).unwrap()
  })
  for round in 0..<7 {
    let collector = @latency.Collector::new(@latency.Milliseconds, 1L, 1024).unwrap()
    let start = @clock.monotonic_clock_start()
    if batch_size == 1 {
      for packet in packets {
        collector.receive_bytes(packet).unwrap() |> ignore
      }
    } else {
      for batch in batches {
        collector.receive_batch_bytes(batch).unwrap() |> ignore
      }
    }
    let elapsed = @clock.monotonic_clock_end(start)
    if collector.summary().count() != 192L * buckets.to_int64() {
      abort("ingestion count mismatch")
    }
    println("\{label},\{round},\{elapsed}")
  }
}

///|
fn main {
  println("case,round,microseconds")
  run("sparse_single", 3, 1)
  run("dense_single", 640, 1)
  run("sparse_batch_2", 3, 2)
  run("dense_batch_8", 640, 8)
}
