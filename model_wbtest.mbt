///|
priv struct ModelSample {
  source : Int64
  sequence : Int64
  timestamp : Int64
  value : Int64
  weight : Int64
}

///|
priv struct Model {
  width : Int64
  slots : Int64
  capacity : Int32
  mut latest : Int64
  mut live : Array[ModelSample]
}

///|
priv enum ModelVerdict {
  ModelAdded
  ModelDuplicate
  ModelConflict
  ModelTooOld
  ModelFull
}

///|
fn Model::new(width : Int64, slots : Int32, capacity : Int32) -> Model {
  { width, slots: slots.to_int64(), capacity, latest: -1L, live: [], }
}

///|
fn Model::advance(self : Model, time : Int64) -> Unit {
  self.latest = time / self.width
  self.live = self.live.filter(sample => {
    self.latest - sample.timestamp / self.width < self.slots
  })
}

///|
fn Model::apply(self : Model, sample : ModelSample) -> ModelVerdict {
  for accepted in self.live {
    if accepted.source == sample.source && accepted.sequence == sample.sequence {
      // Workloads below use exact buckets for identities that may collide.
      if accepted.timestamp == sample.timestamp &&
        accepted.value == sample.value &&
        accepted.weight == sample.weight {
        return ModelDuplicate
      }
      return ModelConflict
    }
  }
  let epoch = sample.timestamp / self.width
  if self.latest - epoch >= self.slots {
    return ModelTooOld
  }
  let staged_epoch = if epoch > self.latest { epoch } else { self.latest }
  let retained = self.live.filter(item => {
    staged_epoch - item.timestamp / self.width < self.slots
  })
  if retained.length() >= self.capacity {
    return ModelFull
  }
  self.latest = staged_epoch
  retained.push(sample)
  self.live = retained
  ModelAdded
}

///|
fn Model::exact(self : Model) -> Array[Int64] {
  let result = []
  for item in self.live {
    for _ in 0..<item.weight.to_int() {
      result.push(item.value)
    }
  }
  result.sort()
  result
}

///|
fn verify_model(model : Model, collector : Collector) -> Unit raise {
  let sorted = model.exact()
  let distribution = collector.summary().distribution()
  assert_eq(distribution.count(), sorted.length().to_int64())
  if sorted.is_empty() {
    assert_true(distribution.quantile(500000) is None)
  } else {
    for p in [0, 10000, 500000, 950000, 990000, 1000000] {
      let index = if p == 0 {
        0
      } else {
        ((sorted.length().to_int64() * p.to_int64() + 999999L) / 1000000L).to_int() -
        1
      }
      let match_value = distribution.quantile(p).unwrap()
      assert_true(
        match_value.lower <= sorted[index] && sorted[index] <= match_value.upper,
      )
    }
  }
  for threshold in [0L, 12L, 31L, 32L, 399L, 400L, 500L, MAX_COUNT] {
    let mut exact = 0L
    for value in sorted {
      if value <= threshold {
        exact += 1L
      }
    }
    let result = distribution.count_at_most(threshold)
    assert_true(result.lower <= exact && exact <= result.upper)
  }
}

///|
fn matches_model(
  result : Result[Receipt, CollectError],
  expected : ModelVerdict,
) -> Bool {
  match (result, expected) {
    (Ok(Added), ModelAdded)
    | (Ok(Duplicate), ModelDuplicate)
    | (Err(ConflictingIdentity), ModelConflict)
    | (Err(WindowFailure(ClockWentBackward)), ModelTooOld)
    | (Err(IdentityBudget), ModelFull) => true
    _ => false
  }
}

///|
test "collector matches a flat model through lossy retry and restore traces" {
  for
    (width, slots, capacity) in [
      (1L, 1, 3),
      (7L, 3, 5),
      (13L, 8, 24),
      (11L, 17, 40),
    ] {
    let model = Model::new(width, slots, capacity)
    let mut actual = Collector::new(
      Milliseconds,
      width,
      slots,
      identities=capacity,
    ).unwrap()
    let mut logical_time = 0L
    let mut state = 99991U
    for step in 0..<400 {
      state = state * 1664525U + 1013904223U
      if step % 37 == 0 {
        logical_time += 55L
        model.advance(logical_time)
        actual.advance(logical_time).unwrap()
      } else {
        logical_time += ((state >> 21) % 3U).reinterpret_as_int().to_int64()
        let late = ((state >> 12) % 15U).reinterpret_as_int().to_int64()
        let latest_time = if logical_time >= late {
          logical_time - late
        } else {
          0L
        }
        let sample = if model.live.length() > 0 && step % 9 == 0 {
          model.live[step % model.live.length()]
        } else if model.live.length() > 0 && step % 14 == 0 {
          let old = model.live[step % model.live.length()]
          { ..old, weight: old.weight + 1L, }
        } else {
          {
            source: (step % 7).to_int64(),
            sequence: step.to_int64(),
            timestamp: latest_time,
            value: (if step % 3 == 0 { 400 + step } else { step % 32 }).to_int64(),
            weight: (step % 5 + 1).to_int64(),
          }
        }
        let encoded = packet(
          sample.source,
          sample.sequence,
          sample.timestamp,
          Milliseconds,
          sample.value,
          sample.weight,
        ).encode()
        let expected = model.apply(sample)
        assert_true(matches_model(actual.receive_bytes(encoded), expected))
      }
      if step % 23 == 0 {
        actual = Collector::restore(actual.checkpoint()).unwrap()
      }
      verify_model(model, actual)
    }
  }
}
