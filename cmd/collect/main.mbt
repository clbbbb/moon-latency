///|
#module("node:fs")
extern "js" fn open_file(path : String, mode : String) -> Int32 = "openSync"

///|
#module("node:fs")
extern "js" fn close_file(fd : Int32) -> Unit = "closeSync"

///|
type FileBuffer

///|
#module("node:fs")
extern "js" fn read_file(
  fd : Int32,
  buffer : FileBuffer,
  offset : Int32,
  length : Int32,
) -> Int32 = "readSync"

///|
extern "js" fn read_shard(
  path : String,
  open : (String, String) -> Int32,
  read : (Int32, FileBuffer, Int32, Int32) -> Int32,
  close : (Int32) -> Unit,
) -> String =
  #| (path, open, read, close) => {
  #|   try {
  #|     const chunk = Buffer.alloc(65536);
  #|     const fd = open(path, "r");
  #|     try {
  #|       const parts = [];
  #|       let length = 0;
  #|       for (;;) {
  #|         const n = read(fd, chunk, 0, Math.min(chunk.length, 9874439 - length));
  #|         if (n === 0) return "=" + parts.join("");
  #|         length += n;
  #|         if (length > 9874438) return "!input exceeds the maximum batch size";
  #|         parts.push(chunk.subarray(0, n).toString("hex"));
  #|       }
  #|     } finally { close(fd); }
  #|   } catch (e) { return "!" + e.message; }
  #| }

///|
extern "js" fn fail(message : String) -> Unit =
  #| message => { console.error(message); process.exitCode = 2; }

///|
fn parse_parameters(args : Array[String]) -> (Int64, Int32, Int64, Int32)? {
  Some(
    (
      @string.parse_int64(args[1][:]),
      @string.parse_int(args[2][:]),
      @string.parse_int64(args[3][:]),
      @string.parse_int(args[4][:]),
    ),
  ) catch {
    _ => None
  }
}

///|
fn run(args : Array[String]) -> Result[Unit, String] {
  if args == ["--help"] {
    println(
      "collect <ns|us|ms> <epoch-width> <slots> <threshold> <target-ppm> <shard>...",
    )
    println("collect fleet <config.json> <service=file>...")
    return Ok(())
  }
  if !args.is_empty() && args[0] == "fleet" {
    return run_fleet(args)
  }
  if !args.is_empty() && args[0] == "journal" {
    return run_journal(args[1:].to_owned())
  }
  if args.length() < 6 {
    return Err("missing parameters or shard files; use --help")
  }
  let unit = match @latency.LatencyUnit::parse(args[0]) {
    Some(unit) => unit
    None => return Err("unit must be ns, us, or ms")
  }
  let (width, slots, threshold, target) = match parse_parameters(args) {
    None => return Err("invalid integer argument")
    Some(values) => values
  }
  if threshold < 0L || target < 0 || target > 1000000 {
    return Err("invalid threshold or target")
  }
  let collector = match @latency.Collector::new(unit, width, slots) {
    Err(_) => return Err("invalid window")
    Ok(value) => value
  }
  let mut added = 0
  let mut duplicate = 0
  for i in 5..<args.length() {
    let path = args[i]
    let input = read_shard(path, open_file, read_file, close_file)
    if input.has_prefix("!") {
      return Err(path + ": " + input[1:].to_owned())
    }
    let payload = @hex.decode(input[1:].to_owned()) catch {
      _ => return Err(path + ": invalid hexadecimal data")
    }
    if payload.length() >= 4 && payload[:4].to_owned() == b"MLB1" {
      match collector.receive_batch_bytes(payload) {
        Ok(receipt) => {
          added += receipt.added
          duplicate += receipt.duplicates
        }
        Err(@latency.RejectedShard(index, error)) =>
          return Err(
            path +
            ": batch record " +
            index.to_string() +
            ": " +
            collect_message(error),
          )
        Err(_) => return Err(path + ": invalid batch encoding")
      }
      continue
    }
    match collector.receive_bytes(payload) {
      Ok(@latency.Added) => added += 1
      Ok(@latency.Duplicate) => duplicate += 1
      Err(error) => return Err(path + ": " + collect_message(error))
    }
  }
  let report = collector.summary().assess_slo(threshold, target).unwrap()
  let status = report.status.name()
  // Int64 totals are decimal strings so JavaScript consumers retain precision.
  println(
    "{\"status\":\"\{status}\",\"total\":\"\{report.total}\",\"lower\":\"\{report.passing.lower}\",\"upper\":\"\{report.passing.upper}\",\"required\":\"\{report.required}\",\"added\":\{added},\"duplicate\":\{duplicate}}",
  )
  Ok(())
}

///|
fn collect_message(error : @latency.CollectError) -> String {
  match error {
    @latency.IncompatibleTimeBucket =>
      "shard spans more than one collector time bucket"
    @latency.UnitMismatch => "shard latency unit mismatch"
    @latency.ConflictingIdentity => "conflicting shard identity"
    @latency.IdentityBudget => "shard identity budget exceeded"
    @latency.ShardFailure(_) => "invalid shard encoding"
    @latency.WindowFailure(@latency.ClockWentBackward) =>
      "shard is older than the active window"
    @latency.WindowFailure(@latency.CountOverflow) => "window count overflow"
    @latency.WindowFailure(_) => "invalid window operation"
  }
}

///|
fn main {
  let args = @env.args()
  match run(args[2:].to_owned()) {
    Ok(_) => ()
    Err(message) => fail(message)
  }
}
