import random

import pytest

from tesseract.recorder.backoff import Backoff


def test_exponential_ceiling_capped_at_60():
    b = Backoff(1.0, 60.0)
    assert [b.ceiling(i) for i in range(8)] == [1, 2, 4, 8, 16, 32, 60, 60]
    assert b.ceiling(10_000) == 60  # no overflow


def test_delays_within_jitter_bounds_and_cap():
    b = Backoff(1.0, 60.0, rng=random.Random(42))
    for attempt in range(50):
        ceil = b.ceiling(attempt)
        d = b.next_delay()
        assert ceil / 2 <= d <= ceil <= 60


def test_jitter_actually_varies():
    b = Backoff(1.0, 60.0, rng=random.Random(1))
    delays = set()
    for _ in range(20):
        b.attempt = 10  # pinned at the cap
        delays.add(round(b.next_delay(), 6))
    assert len(delays) > 10


def test_reset():
    b = Backoff(0.5, 60.0, rng=random.Random(0))
    for _ in range(5):
        b.next_delay()
    b.reset()
    assert b.attempt == 0
    assert b.next_delay() <= 0.5


def test_invalid_args():
    with pytest.raises(ValueError):
        Backoff(0, 60)
