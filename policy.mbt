///|
pub(all) enum PolicyError {
  EmptyPolicy
  TooManyObjectives
  InvalidObjective(Int32)
  DuplicateObjective(Int32)
}

///|
pub(all) struct Objective {
  name : String
  threshold : Int64
  target : Int32
}

///|
pub(all) struct ObjectiveResult {
  objective : Objective
  report : SloReport
  // Number of failures the target permits and the observed failure bounds.
  allowed_failures : Int64
  observed_failures : Interval
}

///|
pub struct Policy {
  priv ordered : Array[Objective]
}

///|
pub fn Policy::new(
  objectives : Array[Objective],
) -> Result[Policy, PolicyError] {
  if objectives.is_empty() {
    return Err(EmptyPolicy)
  }
  if objectives.length() > 256 {
    return Err(TooManyObjectives)
  }
  let names : Map[String, Unit] = Map([])
  for i in 0..<objectives.length() {
    let objective = objectives[i]
    if objective.name.length() == 0 ||
      objective.name.length() > 128 ||
      objective.threshold < 0L ||
      objective.target < 0 ||
      objective.target > 1000000 {
      return Err(InvalidObjective(i))
    }
    if names.get(objective.name) is Some(_) {
      return Err(DuplicateObjective(i))
    }
    names[objective.name] = ()
  }
  Ok({ ordered: objectives.copy(), })
}

///|
pub fn Policy::evaluate(
  self : Policy,
  distribution : Distribution,
) -> Array[ObjectiveResult] {
  self.ordered.map(objective => {
    let total = distribution.count()
    let passing = distribution.count_at_most(objective.threshold)
    let required = ceil_fraction(total, objective.target)
    {
      objective,
      report: make_report(total, passing, required),
      allowed_failures: total - required,
      observed_failures: {
        lower: total - passing.upper,
        upper: total - passing.lower,
      },
    }
  })
}

///|
/// Aggregate evaluation across a before/after deployment without averaging
/// the fractions from different sample sizes.
pub(all) struct ObjectiveTransition {
  previous : ObjectiveResult
  current : ObjectiveResult
}

///|
pub fn Policy::compare(
  self : Policy,
  previous : Distribution,
  current : Distribution,
) -> Array[ObjectiveTransition] {
  let before = self.evaluate(previous)
  let after = self.evaluate(current)
  Array::makei(before.length(), i => { previous: before[i], current: after[i], })
}
