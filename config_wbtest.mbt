///|
fn service_json(
  name : String,
  unit : String,
  width : String,
  slots : Int32,
  identities : Int32,
  objectives : Array[Json],
) -> Json {
  Json::object(
    Map([
      ("service", Json::string(name)),
      ("unit", Json::string(unit)),
      ("width", Json::string(width)),
      ("slots", Json::number(slots.to_double())),
      ("identities", Json::number(identities.to_double())),
      ("objectives", Json::array(objectives)),
    ]),
  )
}

///|
fn objective_json(name : String, bound : String, target : Double) -> Json {
  Json::object(
    Map([
      ("name", Json::string(name)),
      ("threshold", Json::string(bound)),
      ("target", Json::number(target)),
    ]),
  )
}

///|
fn configuration(rows : Array[Json]) -> String {
  Json::object(
    Map([("version", Json::string("1")), ("services", Json::array(rows))]),
  ).stringify()
}

///|
test "deployment roundtrip preserves units 64-bit limits and objective order" {
  let maximum = MAX_COUNT.to_string()
  let data = configuration([
    service_json("api", "ms", maximum, 2, 3, [
      objective_json("p99", "400", 990000.0),
      objective_json("all", maximum, 1000000.0),
    ]),
    service_json("queue", "us", "1", 1, 1, [
      objective_json("p50", "32", 500000.0),
    ]),
  ])
  let deployment = Deployment::parse(data).unwrap()
  let wire = deployment.to_json()
  assert_eq(Deployment::parse(wire).unwrap().to_json(), wire)
  assert_true(wire.contains(maximum))
  let registry = deployment.registry()
  registry
  .receive_bytes(
    "api",
    packet(0L, 0L, 0L, Milliseconds, 400L, MAX_COUNT).encode(),
  )
  .unwrap()
  |> ignore
  registry
  .receive_bytes("queue", packet(0L, 0L, 0L, Microseconds, 33L, 5L).encode())
  .unwrap()
  |> ignore
  let reports = deployment.report(registry).unwrap()
  assert_eq(reports.length(), 2)
  assert_eq(reports[0].objectives[0].objective.name, "p99")
  assert_true(reports[0].objectives[0].report.status is Indeterminate)
  assert_true(reports[0].objectives[1].report.status is Met)
  assert_true(reports[1].objectives[0].report.status is Indeterminate)
  assert_true(
    deployment.report_json(registry).unwrap().contains("\"" + maximum + "\""),
  )
  assert_true(registry.remove("api"))
  assert_true(deployment.report(registry) is Err(UnknownService))
}

///|
test "configuration rejects malformed decimals foreign fields and invalid targets" {
  let rule = [objective_json("budget", "32", 1000000.0)]
  for width in ["", "0", "-1", "+1", "01", "9223372036854775808"] {
    assert_true(
      Deployment::parse(
        configuration([service_json("api", "us", width, 1, 1, rule)]),
      )
      is Err(InvalidServiceConfig(0)),
    )
  }
  for threshold in ["-1", "+1", "01", "9223372036854775808"] {
    assert_true(
      Deployment::parse(
        configuration([
          service_json("api", "us", "1", 1, 1, [
            objective_json("p99", threshold, 990000.0),
          ]),
        ]),
      )
      is Err(InvalidServiceConfig(0)),
    )
  }
  for target in [-1.0, 1.5, 1000001.0] {
    assert_true(
      Deployment::parse(
        configuration([
          service_json("api", "us", "1", 1, 1, [
            objective_json("p99", "32", target),
          ]),
        ]),
      )
      is Err(InvalidServiceConfig(0)),
    )
  }
  assert_true(
    Deployment::parse("{\"version\":\"1\",\"version\":\"1\",\"services\":[]}")
    is Err(ConfigSyntax),
  )
  assert_true(
    Deployment::parse("{\"version\":\"2\",\"services\":[]}")
    is Err(ConfigVersion),
  )
  assert_true(
    Deployment::parse("{\"version\":\"1\",\"typo\":[]}") is Err(ConfigSyntax),
  )
}

///|
test "deployment checks aggregate quotas and duplicate names before allocation" {
  let rule = [objective_json("budget", "32", 999000.0)]
  let one = service_json("first", "us", "1", 2, 4096, rule)
  let two = service_json("second", "ms", "2", 2, 4096, rule)
  let three = service_json("third", "ns", "3", 1, 1, rule)
  assert_true(
    Deployment::parse(configuration([one, one]))
    is Err(ServiceRejected(1, DuplicateService)),
  )
  assert_true(
    Deployment::parse(configuration([one, two, three]))
    is Err(ServiceRejected(2, ServiceBudget)),
  )
  assert_true(Deployment::parse(configuration([one, two])) is Ok(_))
}
