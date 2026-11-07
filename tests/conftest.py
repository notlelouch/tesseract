from pathlib import Path

import pytest

FIXTURES = Path(__file__).parent / "fixtures"


def fixture_text(name: str) -> str:
    return (FIXTURES / name).read_text().rstrip("\n")


def fixture_lines(name: str) -> list[str]:
    return [l for l in (FIXTURES / name).read_text().splitlines() if l.strip()]


class FakeClock:
    """Monotonic clock + sleep that advances time instantly."""

    def __init__(self, t: float = 1000.0):
        self.t = t
        self.sleeps: list[float] = []

    def __call__(self) -> float:
        return self.t

    async def sleep(self, s: float) -> None:
        self.sleeps.append(s)
        self.t += s


class FixedNsClock:
    """Wall clock (ns) pinned to a fixed instant; each read advances it by `step_ns`, so
    start < end and ordering checks behave, but the values never depend on today's date."""

    def __init__(self, t0_ns: int, step_ns: int = 1_000_000):
        self.t = t0_ns
        self.step = step_ns

    def __call__(self) -> int:
        t = self.t
        self.t += self.step
        return t


@pytest.fixture
def clock() -> FakeClock:
    return FakeClock()
