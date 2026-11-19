///|
test "bucket boundaries are adjacent across the entire Int64 domain" {
  assert_eq(bounds.length(), 960)
  for i in 0..<bounds.length() {
    assert_eq(bucket(bounds[i]), i)
    assert_eq(bucket(upper_at(i)), i)
    if i > 0 {
      assert_eq(upper_at(i - 1) + 1L, bounds[i])
      assert_true(upper_at(i) - bounds[i] <= bounds[i] / 16L)
    }
  }
}
