///|
pub(all) enum ConfigError {
  ConfigSize
  ConfigSyntax
  ConfigVersion
  InvalidServiceConfig(Int32)
  ServiceRejected(Int32, RegistryError)
  ObjectiveRejected(Int32, PolicyError)
}

///|
priv struct DeploymentService {
  config : ServiceConfig
  objectives : Array[Objective]
  policy : Policy
}

///|
/// Validated per-service configurations and policies for a collector deployment.
pub struct Deployment {
  priv configs : Array[DeploymentService]
}

///|
fn object_fields(json : Json, size : Int32) -> Map[String, Json]? {
  match json {
    Object(fields) if fields.length() == size => Some(fields)
    _ => None
  }
}

///|
fn parse_decimal(field : Json?) -> Int64? {
  guard field is Some(String(value)) else { return None }
  if value.is_empty() ||
    value.length() > 19 ||
    (value.length() > 1 && value.has_prefix("0")) {
    return None
  }
  for ch in value {
    if ch < '0' || ch > '9' {
      return None
    }
  }
  Some(@string.parse_int64(value[:])) catch {
    _ => None
  }
}

///|
fn parse_small(field : Json?, max : Int32) -> Int32? {
  guard field is Some(Number(value, ..)) else { return None }
  if value < 0.0 || value > max.to_double() {
    return None
  }
  let integer = value.to_int()
  if integer.to_double() != value {
    None
  } else {
    Some(integer)
  }
}

///|
pub fn LatencyUnit::name(self : LatencyUnit) -> String {
  match self {
    Nanoseconds => "ns"
    Microseconds => "us"
    Milliseconds => "ms"
  }
}

///|
pub fn LatencyUnit::parse(name : String) -> LatencyUnit? {
  match name {
    "ns" => Some(Nanoseconds)
    "us" => Some(Microseconds)
    "ms" => Some(Milliseconds)
    _ => None
  }
}

///|
fn read_objective(row : Json) -> Objective? {
  guard object_fields(row, 3) is Some(fields) else { return None }
  guard fields.get("name") is Some(String(name)) else { return None }
  guard parse_decimal(fields.get("threshold")) is Some(threshold) else {
    return None
  }
  guard parse_small(fields.get("target"), 1000000) is Some(target) else {
    return None
  }
  Some({ name, threshold, target, })
}

///|
fn read_service(row : Json) -> (ServiceConfig, Array[Objective])? {
  guard object_fields(row, 6) is Some(fields) else { return None }
  guard fields.get("service") is Some(String(service)) else { return None }
  guard fields.get("unit") is Some(String(unit_name)) else { return None }
  guard LatencyUnit::parse(unit_name) is Some(unit) else { return None }
  guard parse_decimal(fields.get("width")) is Some(width) else { return None }
  guard parse_small(fields.get("slots"), 1024) is Some(slots) else {
    return None
  }
  guard parse_small(fields.get("identities"), 4096) is Some(identities) else {
    return None
  }
  guard fields.get("objectives") is Some(Array(items)) else { return None }
  if items.length() > 256 {
    return None
  }
  let objectives = []
  for item in items {
    guard read_objective(item) is Some(objective) else { return None }
    objectives.push(objective)
  }
  Some(({ service, unit, width, slots, identities, }, objectives))
}

///|
pub fn Deployment::parse(text : String) -> Result[Deployment, ConfigError] {
  if text.length() > 1048576 {
    return Err(ConfigSize)
  }
  let data = @json.parse(
    text[:],
    max_nesting_depth=10,
    reject_duplicate_keys=true,
  ) catch {
    _ => return Err(ConfigSyntax)
  }
  guard object_fields(data, 2) is Some(root) else { return Err(ConfigSyntax) }
  if root.get("version") != Some(Json::string("1")) {
    return Err(ConfigVersion)
  }
  guard root.get("services") is Some(Array(rows)) else {
    return Err(ConfigSyntax)
  }
  if rows.length() < 1 || rows.length() > 256 {
    return Err(ConfigSize)
  }
  let configs : Array[DeploymentService] = []
  // Validate aggregate capacities without allocating any collectors.
  let names : Map[String, Unit] = Map([])
  let mut slots = 0
  let mut identities = 0
  for i in 0..<rows.length() {
    guard read_service(rows[i]) is Some((config, objectives)) else {
      return Err(InvalidServiceConfig(i))
    }
    if !valid_service_name(config.service) ||
      config.width <= 0L ||
      config.slots == 0 ||
      config.identities == 0 {
      return Err(InvalidServiceConfig(i))
    }
    if names.get(config.service) is Some(_) {
      return Err(ServiceRejected(i, DuplicateService))
    }
    slots += config.slots
    identities += config.identities
    if slots > 4096 || identities > 8192 {
      return Err(ServiceRejected(i, ServiceBudget))
    }
    names[config.service] = ()
    let policy = match Policy::new(objectives) {
      Ok(policy) => policy
      Err(error) => return Err(ObjectiveRejected(i, error))
    }
    configs.push({ config, objectives, policy, })
  }
  Ok({ configs, })
}

///|
pub fn Deployment::registry(self : Deployment) -> Registry {
  let registry = Registry::new(capacity=self.configs.length()).unwrap()
  for service in self.configs {
    registry.register(service.config).unwrap()
  }
  registry
}

///|
pub fn Deployment::report(
  self : Deployment,
  registry : Registry,
) -> Result[Array[ServiceReport], RegistryError] {
  let results = []
  for service in self.configs {
    match registry.evaluate(service.config.service, service.policy) {
      Ok(report) => {
        if report.unit.tag() != service.config.unit.tag() {
          return Err(RoutedFailure(UnitMismatch))
        }
        results.push(report)
      }
      Err(error) => return Err(error)
    }
  }
  Ok(results)
}

///|
pub fn Deployment::to_json(self : Deployment) -> String {
  let services = self.configs.map(item => {
    let config = item.config
    Json::object(
      Map([
        ("service", Json::string(config.service)),
        ("unit", Json::string(config.unit.name())),
        ("width", Json::string(config.width.to_string())),
        ("slots", Json::number(config.slots.to_double())),
        ("identities", Json::number(config.identities.to_double())),
        (
          "objectives",
          Json::array(
            item.objectives.map(rule => {
              Json::object(
                Map([
                  ("name", Json::string(rule.name)),
                  ("threshold", Json::string(rule.threshold.to_string())),
                  ("target", Json::number(rule.target.to_double())),
                ]),
              )
            }),
          ),
        ),
      ]),
    )
  })
  Json::object(
    Map([("version", Json::string("1")), ("services", Json::array(services))]),
  ).stringify()
}
