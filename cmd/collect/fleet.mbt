///|
extern "js" fn decode_config(hex : String) -> String =
  #| hex => {
  #|   if (hex.length > 2 * 1048576) return "!configuration exceeds 1 MiB";
  #|   try {
  #|     return "=" + new TextDecoder("utf-8", { fatal: true }).decode(Buffer.from(hex, "hex"));
  #|   } catch (error) {
  #|     return "!configuration is not valid UTF-8";
  #|   }
  #| }

///|
fn config_message(error : @latency.ConfigError) -> String {
  match error {
    @latency.ConfigSize => "configuration size or service count exceeds limits"
    @latency.ConfigSyntax => "invalid configuration JSON"
    @latency.ConfigVersion => "unsupported configuration version"
    @latency.InvalidServiceConfig(i) =>
      "invalid service configuration at index " + i.to_string()
    @latency.ServiceRejected(i, why) =>
      "service " + i.to_string() + ": " + registry_message(why)
    @latency.ObjectiveRejected(i, why) =>
      "service " + i.to_string() + ": " + policy_message(why)
  }
}

///|
fn policy_message(error : @latency.PolicyError) -> String {
  match error {
    @latency.EmptyPolicy => "no objectives"
    @latency.TooManyObjectives => "too many objectives"
    @latency.InvalidObjective(i) =>
      "invalid objective at index " + i.to_string()
    @latency.DuplicateObjective(i) =>
      "duplicate objective at index " + i.to_string()
  }
}

///|
fn registry_message(error : @latency.RegistryError) -> String {
  match error {
    @latency.InvalidService => "invalid service name"
    @latency.DuplicateService => "duplicate service name"
    @latency.UnknownService => "unknown service"
    @latency.ServiceBudget => "service budget exceeded"
    @latency.RoutedFailure(error) => collect_message(error)
    @latency.RoutedBatchFailure(@latency.RejectedShard(i, error)) =>
      "batch record " + i.to_string() + ": " + collect_message(error)
    @latency.RoutedBatchFailure(_) => "invalid batch encoding"
  }
}

///|
fn fleet_read(path : String) -> Result[String, String] {
  let input = read_shard(path, open_file, read_file, close_file)
  if input.has_prefix("!") {
    Err(path + ": " + input[1:].to_owned())
  } else {
    Ok(input[1:].to_owned())
  }
}

///|
fn route_parts(arg : String) -> (String, String)? {
  for i in 0..<arg.length() {
    if arg[i] == '=' {
      if i == 0 || i == arg.length() - 1 {
        return None
      }
      return Some((arg[:i].to_owned(), arg[i + 1:].to_owned()))
    }
  }
  None
}

///|
fn run_fleet(args : Array[String]) -> Result[Unit, String] {
  if args.length() < 2 {
    return Err("missing fleet configuration")
  }
  let encoded = match fleet_read(args[1]) {
    Ok(text) => text
    Err(error) => return Err(error)
  }
  let decoded = decode_config(encoded)
  if decoded.has_prefix("!") {
    return Err(args[1] + ": " + decoded[1:].to_owned())
  }
  let config = match @latency.Deployment::parse(decoded[1:].to_owned()) {
    Ok(value) => value
    Err(error) => return Err(args[1] + ": " + config_message(error))
  }
  let registry = config.registry()
  for i in 2..<args.length() {
    let (service, path) = match route_parts(args[i]) {
      Some(pair) => pair
      None => return Err("expected service=file at argument " + i.to_string())
    }
    let payload = match fleet_read(path) {
      Ok(text) =>
        @hex.decode(text) catch {
          _ => return Err(path + ": invalid hexadecimal data")
        }
      Err(error) => return Err(error)
    }
    let result = if payload.length() >= 4 && payload[:4].to_owned() == b"MLB1" {
      registry.receive_batch_bytes(service, payload).map(_ => ())
    } else {
      registry.receive_bytes(service, payload).map(_ => ())
    }
    if result is Err(error) {
      return Err(service + " " + path + ": " + registry_message(error))
    }
  }
  match config.report_json(registry) {
    Ok(report) => {
      println(report)
      Ok(())
    }
    Err(error) => Err(registry_message(error))
  }
}
