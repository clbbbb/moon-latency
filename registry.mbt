///|
pub(all) struct ServiceConfig {
  service : String
  unit : LatencyUnit
  width : Int64
  slots : Int32
  identities : Int32
}

///|
pub(all) enum RegistryError {
  InvalidService
  DuplicateService
  UnknownService
  ServiceBudget
  RoutedFailure(CollectError)
  RoutedBatchFailure(BatchCollectError)
}

///|
pub(all) struct RoutedShard {
  service : String
  shard : Shard
}

///|
pub(all) enum RouteTransactionError {
  TooManyRoutes
  InvalidRoute(Int32, RegistryError)
}

///|
/// Independent clocks and retry identities for named services.
pub struct Registry {
  priv capacity : Int32
  priv services : Map[String, Collector]
  priv mut reserved_slots : Int32
  priv mut reserved_identities : Int32
}

///|
pub fn Registry::new(capacity~ : Int32 = 32) -> Result[Registry, RegistryError] {
  if capacity < 1 || capacity > 256 {
    return Err(ServiceBudget)
  }
  Ok({ capacity, services: Map([]), reserved_slots: 0, reserved_identities: 0, })
}

///|
fn valid_service_name(name : String) -> Bool {
  if name.is_empty() || name.length() > 64 {
    return false
  }
  for ch in name {
    if !(ch >= 'a' && ch <= 'z') &&
      !(ch >= 'A' && ch <= 'Z') &&
      !(ch >= '0' && ch <= '9') &&
      ch != '-' &&
      ch != '_' &&
      ch != '.' {
      return false
    }
  }
  true
}

///|
pub fn Registry::register(
  self : Registry,
  config : ServiceConfig,
) -> Result[Unit, RegistryError] {
  if !valid_service_name(config.service) {
    return Err(InvalidService)
  }
  if self.services.get(config.service) is Some(_) {
    return Err(DuplicateService)
  }
  if self.services.length() >= self.capacity ||
    config.slots > 4096 - self.reserved_slots ||
    config.identities > 8192 - self.reserved_identities {
    return Err(ServiceBudget)
  }
  match
    Collector::new(
      config.unit,
      config.width,
      config.slots,
      identities=config.identities,
    ) {
    Err(error) => Err(RoutedFailure(error))
    Ok(collector) => {
      self.services[config.service] = collector
      self.reserved_slots += config.slots
      self.reserved_identities += config.identities
      Ok(())
    }
  }
}

///|
pub fn Registry::remove(self : Registry, service : String) -> Bool {
  match self.services.get(service) {
    None => false
    Some(collector) => {
      self.reserved_slots -= collector.window.slots.length()
      self.reserved_identities -= collector.capacity
      self.services.remove(service)
      true
    }
  }
}

///|
pub fn Registry::names(self : Registry) -> Array[String] {
  let names = []
  for name, _ in self.services {
    names.push(name)
  }
  names.sort()
  names
}

///|
pub fn Registry::receive_bytes(
  self : Registry,
  service : String,
  bytes : Bytes,
) -> Result[Receipt, RegistryError] {
  match self.services.get(service) {
    None => Err(UnknownService)
    Some(collector) =>
      collector.receive_bytes(bytes).map_err(error => RoutedFailure(error))
  }
}

///|
pub fn Registry::receive_batch_bytes(
  self : Registry,
  service : String,
  bytes : Bytes,
) -> Result[BatchReceipt, RegistryError] {
  match self.services.get(service) {
    None => Err(UnknownService)
    Some(collector) =>
      collector
      .receive_batch_bytes(bytes)
      .map_err(error => RoutedBatchFailure(error))
  }
}

///|
/// A transaction lazily clones only the named services it touches.
pub fn Registry::receive_atomic(
  self : Registry,
  routes : Array[RoutedShard],
) -> Result[BatchReceipt, RouteTransactionError] {
  if routes.length() > BATCH_LIMIT {
    return Err(TooManyRoutes)
  }
  let staged : Map[String, Collector] = Map([])
  let mut added = 0
  let mut duplicates = 0
  for i in 0..<routes.length() {
    let route = routes[i]
    let collector = match staged.get(route.service) {
      Some(collector) => collector
      None =>
        match self.services.get(route.service) {
          None => return Err(InvalidRoute(i, UnknownService))
          Some(original) => {
            let copy = original.copy()
            staged[route.service] = copy
            copy
          }
        }
    }
    match collector.receive(route.shard) {
      Err(error) => return Err(InvalidRoute(i, RoutedFailure(error)))
      Ok(Added) => added += 1
      Ok(Duplicate) => duplicates += 1
    }
  }
  for name, collector in staged {
    self.services[name] = collector
  }
  Ok({ added, duplicates, })
}

///|
pub fn Registry::advance(
  self : Registry,
  service : String,
  timestamp : Int64,
) -> Result[Unit, RegistryError] {
  match self.services.get(service) {
    None => Err(UnknownService)
    Some(collector) =>
      collector.advance(timestamp).map_err(error => RoutedFailure(error))
  }
}

///|
pub fn Registry::summary(
  self : Registry,
  service : String,
) -> Result[Histogram, RegistryError] {
  match self.services.get(service) {
    None => Err(UnknownService)
    Some(collector) => Ok(collector.summary())
  }
}

///|
pub(all) struct ServiceReport {
  service : String
  unit : LatencyUnit
  objectives : Array[ObjectiveResult]
}

///|
/// Evaluate objectives for a service without mixing incompatible units.
pub fn Registry::evaluate(
  self : Registry,
  service : String,
  policy : Policy,
) -> Result[ServiceReport, RegistryError] {
  match self.services.get(service) {
    None => Err(UnknownService)
    Some(collector) =>
      Ok({
        service,
        unit: collector.unit,
        objectives: policy.evaluate(collector.window.aggregate.distribution()),
      })
  }
}
