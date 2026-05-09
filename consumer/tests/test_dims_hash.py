from consumer.apply.dims import _hash


def test_hash_stable_for_same_inputs():
    assert _hash(("ACME", "Street 1", True)) == _hash(("ACME", "Street 1", True))


def test_hash_changes_when_attribute_changes():
    a = _hash(("ACME", "Street 1", True))
    b = _hash(("ACME", "Street 2", True))
    c = _hash(("ACME", "Street 1", False))
    assert a != b
    assert a != c
    assert b != c


def test_hash_treats_none_distinctly():
    assert _hash(("ACME", None, True)) != _hash(("ACME", "", True))
