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
}
