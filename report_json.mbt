///|
pub fn SloStatus::name(self : SloStatus) -> String {
  match self {
    Met => "met"
    Missed => "missed"
    Indeterminate => "indeterminate"
    NoSamples => "no_samples"
  }
}

///|
fn decimal_json(value : Int64) -> Json {
  Json::string(value.to_string())
}

///|
fn interval_json(bounds : Interval) -> Json {
  Json::object(
    Map([
      ("lower", decimal_json(bounds.lower)),
      ("upper", decimal_json(bounds.upper)),
    ]),
  )
}

///|
pub fn SloReport::to_json(self : SloReport) -> Json {
  Json::object(
    Map([
      ("status", Json::string(self.status.name())),
      ("total", decimal_json(self.total)),
      ("required", decimal_json(self.required)),
      ("passing", interval_json(self.passing)),
    ]),
  )
}

///|
pub fn EpochReport::to_json(self : EpochReport) -> Json {
  Json::object(
    Map([
      ("epoch", decimal_json(self.epoch)),
      ("started_at", decimal_json(self.started_at)),
      ("result", self.result.to_json()),
    ]),
  )
}

///|
pub fn ObjectiveResult::to_json(self : ObjectiveResult) -> Json {
  Json::object(
    Map([
      ("name", Json::string(self.objective.name)),
      ("threshold", decimal_json(self.objective.threshold)),
      ("target", Json::number(self.objective.target.to_double())),
      ("report", self.report.to_json()),
      ("allowed_failures", decimal_json(self.allowed_failures)),
      ("observed_failures", interval_json(self.observed_failures)),
    ]),
  )
}

///|
pub fn ServiceReport::to_json(self : ServiceReport) -> Json {
  Json::object(
    Map([
      ("service", Json::string(self.service)),
      ("unit", Json::string(self.unit.name())),
      ("objectives", Json::array(self.objectives.map(item => item.to_json()))),
    ]),
  )
}

///|
pub fn Deployment::report_json(
  self : Deployment,
  registry : Registry,
) -> Result[String, RegistryError] {
  self
  .report(registry)
  .map(reports => {
    Json::object(
      Map([
        ("version", Json::string("1")),
        ("services", Json::array(reports.map(item => item.to_json()))),
      ]),
    ).stringify()
  })
}
