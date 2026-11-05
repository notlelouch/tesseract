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


@pytest.fixture
def clock() -> FakeClock:
    return FakeClock()
