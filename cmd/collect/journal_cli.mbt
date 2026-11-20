///|
extern "js" fn write_binary(hex : String) -> String =
  #| hex => {
  #|   try {
  #|     const fs = require("node:fs");
  #|     const data = Buffer.from(hex, "hex");
  #|     let offset = 0;
  #|     while (offset < data.length) {
  #|       const count = fs.writeSync(1, data, offset, data.length - offset);
  #|       if (count === 0) return "cannot write journal bytes";
  #|       offset += count;
  #|     }
  #|     return "";
  #|   } catch (error) { return error.message; }
  #| }

///|
extern "js" fn stream_journal(
  path : String,
  open : (String, String) -> Int32,
  read : (Int32, FileBuffer, Int32, Int32) -> Int32,
  close : (Int32) -> Unit,
  chunk : (String) -> String,
) -> String =
  #| (path, open, read, close, chunk) => {
  #|   try {
  #|     const fd = open(path, "r");
  #|     try {
  #|       const buffer = Buffer.alloc(65536);
  #|       for (;;) {
  #|         const n = read(fd, buffer, 0, buffer.length);
  #|         if (n === 0) return "";
  #|         const failure = chunk(buffer.subarray(0, n).toString("hex"));
  #|         if (failure) return failure;
  #|       }
  #|     } finally { close(fd); }
  #|   } catch (error) { return error.message; }
  #| }

///|
fn output_frame(frame : Bytes) -> Result[Unit, String] {
  let error = write_binary(@hex.encode(frame[:]))
  if error.is_empty() {
    Ok(())
  } else {
    Err(error)
  }
}

///|
fn pack_journal(paths : Array[String]) -> Result[Unit, String] {
  let writer = @latency.JournalWriter::new().unwrap()
  if output_frame(@latency.journal_header()) is Err(error) {
    return Err(error)
  }
  for path in paths {
    let text = match fleet_read(path) {
      Ok(data) => data
      Err(error) => return Err(error)
    }
    let binary = @hex.decode(text) catch {
      _ => return Err(path + ": invalid hexadecimal input")
    }
    let shard = match @latency.Shard::decode(binary) {
      Ok(shard) => shard
      Err(_) => return Err(path + ": invalid shard encoding")
    }
    match writer.push(shard) {
      Err(_) => return Err(path + ": unable to append shard")
      Ok(Some(frame)) =>
        if output_frame(frame) is Err(error) {
          return Err(error)
        }
      Ok(None) => ()
    }
  }
  match writer.finish() {
    Ok(Some(last)) => output_frame(last)
    Ok(None) => Ok(())
    Err(_) => Err("unable to finalize journal")
  }
}

///|
fn journal_message(error : @latency.JournalError) -> String {
  match error {
    @latency.JournalClosed => "journal reader is closed"
    @latency.BadJournalMagic => "invalid journal magic"
    @latency.BadFrameLength(i) =>
      "invalid length for journal frame " + i.to_string()
    @latency.BadFrameChecksum(i) =>
      "checksum mismatch in journal frame " + i.to_string()
    @latency.UnexpectedEnd(i) =>
      "unexpected EOF at journal frame " + i.to_string()
    @latency.OversizedChunk => "journal read chunk too large"
    @latency.BadJournalBatch(frame, @latency.RejectedShard(index, error)) =>
      "frame " +
      frame.to_string() +
      ", shard " +
      index.to_string() +
      ": " +
      collect_message(error)
    @latency.BadJournalBatch(i, _) =>
      "invalid batch in journal frame " + i.to_string()
  }
}

///|
fn replay_file(
  reader : @latency.JournalReader,
  path : String,
) -> Result[Unit, String] {
  let failure = stream_journal(path, open_file, read_file, close_file, chunk => {
    let bytes = @hex.decode(chunk) catch {
      _ => return "invalid hexadecimal bytes"
    }
    match reader.feed(bytes[:], _ => ()) {
      Ok(_) => ""
      Err(error) => journal_message(error)
    }
  })
  if !failure.is_empty() {
    return Err(path + ": " + failure)
  }
  reader.finish().map_err(error => path + ": " + journal_message(error))
}

///|
fn replay_command(args : Array[String]) -> Result[Unit, String] {
  if args.length() != 7 {
    return Err("expected a unit, window configuration, target and journal path")
  }
  let unit = match @latency.LatencyUnit::parse(args[1]) {
    Some(value) => value
    None => return Err("unit must be ns, us, or ms")
  }
  let params = ["", args[2], args[3], args[4], args[5]]
  let (width, slots, threshold, target) = match parse_parameters(params) {
    Some(values) => values
    None => return Err("invalid replay arguments")
  }
  if threshold < 0L || target < 0 || target > 1000000 {
    return Err("invalid threshold or target")
  }
  let collector = match @latency.Collector::new(unit, width, slots) {
    Ok(value) => value
    Err(error) => return Err(collect_message(error))
  }
  let reader = @latency.JournalReader::new(collector)
  match replay_file(reader, args[6]) {
    Err(error) => return Err(error)
    Ok(_) => ()
  }
  let report = collector.summary().assess_slo(threshold, target).unwrap()
  let epochs = collector.series(threshold, target).unwrap()
  println(
    Json::object(
      Map([
        ("frames", Json::string(reader.frames().to_string())),
        ("summary", report.to_json()),
        ("epochs", Json::array(epochs.map(item => item.to_json()))),
      ]),
    ).stringify(),
  )
  Ok(())
}

///|
fn run_journal(args : Array[String]) -> Result[Unit, String] {
  if args == ["--help"] {
    println("collect journal pack <shard>... > <output.mlj>")
    println(
      "collect journal replay <ns|us|ms> <width> <slots> <threshold> <target-ppm> <input.mlj>",
    )
    return Ok(())
  }
  if args.is_empty() {
    return Err("expected journal pack or replay")
  }
  match args[0] {
    "pack" => pack_journal(args[1:].to_owned())
    "replay" => replay_command(args)
    _ => Err("expected journal pack or replay")
  }
}
