"""Generator invariants that don't need a database."""
import datetime as dt
import random
import re
from decimal import Decimal

from pipeline import generator


def test_phone_formats_are_normalisable():
    rng = random.Random(1)
    for _ in range(200):
        digits = re.sub(r"[^0-9]", "", generator._phone(rng))
        national = digits[2:] if digits.startswith("62") else digits[1:]
        assert re.fullmatch(r"8[0-9]{8,11}", national)


def test_payment_matches_valid_lines():
    rng = random.Random(7)
    now = dt.datetime(2026, 10, 31, 12, 0)
    products = generator._products(rng, 1, now - dt.timedelta(days=30))
    for i in range(300):
        order, items, payments, _, _ = generator._order_rows(
            rng, 1000 + i, 1, 1, 5, now - dt.timedelta(days=rng.uniform(0, 30)), now, products)
        if not payments:
            assert order[2].strip().lower() == "pending"
            continue
        valid = [it for it in items if it[1] == order[0]]
        expected = sum((it[4] * it[3] - it[5] for it in valid), Decimal(0))
        assert payments[0][3] == expected


def test_seeded_rng_is_deterministic():
    a = generator._products(random.Random(42), 1, dt.datetime(2026, 1, 1))
    b = generator._products(random.Random(42), 1, dt.datetime(2026, 1, 1))
    assert a == b
