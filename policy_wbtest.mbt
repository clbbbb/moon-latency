///|
test "policy compares weighted error budgets without averaging each interval" {
  let before = Histogram::new()
  let after = Histogram::new()
  assert_true(before.record(25L, count=198L))
  assert_true(before.record(500L, count=2L))
  assert_true(after.record(25L, count=970L))
  assert_true(after.record(415L, count=30L))
  let policy = Policy::new([
    { name: "tight", threshold: 399L, target: 990000, },
    { name: "loose", threshold: 415L, target: 950000, },
  ]).unwrap()
  let changes = policy.compare(before.distribution(), after.distribution())
  assert_eq(changes.length(), 2)
  assert_eq(changes[0].previous.allowed_failures, 2L)
  assert_eq(changes[0].current.allowed_failures, 10L)
  assert_eq(changes[0].current.observed_failures.lower, 30L)
  assert_eq(changes[0].current.observed_failures.upper, 30L)
  assert_true(changes[0].previous.report.status is Met)
  assert_true(changes[0].current.report.status is Missed)
  assert_true(changes[1].current.report.status is Met)
}

///|
test "policy validates names and owns the rule list" {
  let first = Objective::{ name: "first", threshold: 50L, target: 990000, }
  let objectives = [first]
  let policy = Policy::new(objectives).unwrap()
  objectives[0] = { name: "changed", threshold: 10L, target: 1000000, }
  let results = policy.evaluate(Histogram::new().distribution())
  assert_eq(results[0].objective.name, "first")
  assert_true(results[0].report.status is NoSamples)
  assert_true(Policy::new([first, first]) is Err(DuplicateObjective(1)))
  assert_true(Policy::new([]) is Err(EmptyPolicy))
  assert_true(
    Policy::new([{ name: "", threshold: 0L, target: 1, }])
    is Err(InvalidObjective(0)),
  )
}
