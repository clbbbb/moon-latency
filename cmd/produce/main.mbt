///|
#module("node:fs")
extern "js" fn open_file(path : String, mode : String) -> Int32 = "openSync"

///|
#module("node:fs")
extern "js" fn close_file(fd : Int32) -> Unit = "closeSync"

///|
type Buffer

///|
#module("node:fs")
extern "js" fn read_file(
  fd : Int32,
  buffer : Buffer,
  offset : Int32,
  length : Int32,
) -> Int32 = "readSync"

///|
extern "js" fn read_chunks(
  path : String,
  open : (String, String) -> Int32,
  read : (Int32, Buffer, Int32, Int32) -> Int32,
  close : (Int32) -> Unit,
  consume : (String) -> String,
) -> String =
  #| (path, open, read, close, consume) => {
  #|   try {
  #|     const fd = open(path, "r");
  #|     try {
  #|       const buffer = Buffer.alloc(32768);
  #|       for (;;) {
  #|         const n = read(fd, buffer, 0, buffer.length);
  #|         if (n === 0) return "";
  #|         // Latin-1 is one-to-one. The sample grammar checks delimiters
  #|         // and rejects non-ASCII before interpreting numeric fields.
  #|         const error = consume(buffer.subarray(0, n).toString("latin1"));
  #|         if (error) return error;
  #|       }
  #|     } finally { close(fd); }
  #|   } catch (error) { return error.message; }
  #| }

///|
extern "js" fn fail(message : String) -> Unit =
  #| message => { console.error(message); process.exitCode = 2; }

///|
fn parse_integers(values : Array[String]) -> (Int64, Int64, Int64)? {
  Some(
    (
      @string.parse_int64(values[1][:]),
      @string.parse_int64(values[2][:]),
      @string.parse_int64(values[3][:]),
    ),
  ) catch {
    _ => None
  }
}

///|
fn emit(shard : @latency.Shard) -> Unit {
  println(@hex.encode(shard.encode()[:]))
}

///|
fn run(args : Array[String]) -> Result[Unit, String] {
  if args == ["--help"] {
    println(
      "produce <ns|us|ms> <source> <width> <first-sequence> <samples.csv>",
    )
    return Ok(())
  }
  if args.length() != 5 {
    return Err("expected five arguments; use --help")
  }
  let unit = match @latency.LatencyUnit::parse(args[0]) {
    Some(unit) => unit
    None => return Err("unit must be ns, us, or ms")
  }
  let (source, width, sequence) = match parse_integers(args) {
    Some(values) => values
    None => return Err("invalid integer argument")
  }
  let producer = match @latency.Producer::new(source, unit, width, sequence~) {
    Ok(value) => value
    Err(_) =>
      return Err("expected nonnegative source/sequence and positive width")
  }
  let input = @latency.SampleInput::new(producer)
  let path = args[4]
  let error = read_chunks(path, open_file, read_file, close_file, chunk => {
    match input.feed(chunk, emit) {
      Ok(_) => ""
      Err(error) => input_message(error)
    }
  })
  if !error.is_empty() {
    return Err(path + ": " + error)
  }
  match input.finish() {
    Err(error) => return Err(path + ": " + input_message(error))
    Ok(shards) =>
      for shard in shards {
        emit(shard)
      }
  }
  Ok(())
}

///|
fn input_message(error : @latency.InputError) -> String {
  match error {
    @latency.InputClosed => "input is closed"
    @latency.ChunkTooLarge => "chunk too large"
    @latency.InvalidRow(line) => "invalid CSV row " + line.to_string()
    @latency.LineTooLong(line) => "line too long at row " + line.to_string()
    @latency.InvalidReading(line, error) => {
      let reason = match error {
        @latency.InvalidProducer => "invalid producer"
        @latency.InvalidSample => "invalid sample"
        @latency.SampleOutOfOrder => "sample out of order"
        @latency.SampleCountOverflow => "sample count overflow"
        @latency.SequenceExhausted => "shard sequence exhausted"
        @latency.ProducerFinished => "producer is finished"
      }
      "row " + line.to_string() + ": " + reason
    }
  }
}

///|
fn main {
  match run(@env.args()[2:].to_owned()) {
    Ok(_) => ()
    Err(error) => fail(error)
  }
}
