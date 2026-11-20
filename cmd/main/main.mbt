///|
fn main {
  let east = @latency.Histogram::new()
  let west = @latency.Histogram::new()
  east.record(20L, count=96L) |> ignore
  east.record(405L, count=4L) |> ignore
  west.record(27L, count=98L) |> ignore
  west.record(410L, count=2L) |> ignore
  east.merge(west) |> ignore
  let p99 = east.quantile(990000).unwrap()
  let within = east.count_at_most(400L)
  println("p99: [\{p99.lower}, \{p99.upper}] ms")
  println(
    "requests <= 400 ms: [\{within.lower}, \{within.upper}] / \{east.count()}",
  )
  let window = @latency.Window::new(10L, 3).unwrap()
  let restored = @latency.Histogram::from_snapshot(east.snapshot()).unwrap()
  window.merge_at(10L, restored).unwrap()
  let report = window.summary().assess_slo(400L, 990000).unwrap()
  let status = match report.status {
    @latency.Met => "met"
    @latency.Missed => "missed"
    @latency.Indeterminate => "indeterminate"
    @latency.NoSamples => "no samples"
  }
  println("99% at most 400 ms: \{status} (requires \{report.required})")
  window.advance(40L).unwrap()
  println("samples after expiry: \{window.summary().count()}")
}
