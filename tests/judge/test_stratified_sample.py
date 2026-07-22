from bula_check.judge import stratified_sample


def _items(n, cat, start=0):
    return [{"id": f"q{start + i}", "stress_category": cat} for i in range(n)]


def test_stratified_sample__returns_all_when_n_geq_len():
    items = _items(5, "a")
    assert stratified_sample(items, 10, seed=7) == items


def test_stratified_sample__deterministic_with_seed():
    items = _items(20, "a") + _items(20, "b", start=100)
    first = stratified_sample(items, 8, seed=7)
    second = stratified_sample(items, 8, seed=7)
    assert [i["id"] for i in first] == [i["id"] for i in second]


def test_stratified_sample__covers_categories_proportionally():
    items = _items(30, "a") + _items(10, "b", start=100)
    sample = stratified_sample(items, 8, seed=7)
    assert {i["stress_category"] for i in sample} == {"a", "b"}
    assert len(sample) <= 8


def test_stratified_sample__respects_upper_bound():
    items = _items(50, "a") + _items(50, "b", start=100) + _items(50, "c", start=200)
    assert len(stratified_sample(items, 12, seed=1)) <= 12
