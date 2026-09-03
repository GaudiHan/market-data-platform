"""
Small shared helper for the performance-benchmark suite. Not a pytest file
itself (no test_ prefix) -- imported by the actual benchmark tests.

These benchmarks are deliberately not strict pass/fail gates on absolute
numbers: this sandbox's hardware, your machine, and a CI runner will all
report different raw latencies. What they DO assert is a generous sanity
ceiling (catching a real regression -- e.g. an accidentally-quadratic
change -- without being flaky on slower hardware) and, more importantly,
they print the actual distribution so a human can look at the real numbers.
Run with `pytest tests/performance -v -s` to see the printed output.
"""
from __future__ import annotations

import time
from dataclasses import dataclass


@dataclass
class LatencyStats:
    n: int
    p50_us: float
    p95_us: float
    p99_us: float
    max_us: float
    total_s: float

    @property
    def throughput_per_sec(self) -> float:
        return self.n / self.total_s if self.total_s > 0 else float("inf")

    def __str__(self) -> str:
        return (
            f"n={self.n} total={self.total_s:.3f}s throughput={self.throughput_per_sec:,.0f}/s "
            f"p50={self.p50_us:.1f}us p95={self.p95_us:.1f}us p99={self.p99_us:.1f}us max={self.max_us:.1f}us"
        )


def measure(fn, n: int) -> LatencyStats:
    """Call `fn()` n times, recording per-call latency. Synchronous only --
    see measure_async for coroutine functions."""
    latencies_us = []
    start = time.perf_counter()
    for _ in range(n):
        t0 = time.perf_counter()
        fn()
        latencies_us.append((time.perf_counter() - t0) * 1_000_000)
    total = time.perf_counter() - start
    return _stats(latencies_us, total)


async def measure_async(fn, n: int) -> LatencyStats:
    latencies_us = []
    start = time.perf_counter()
    for _ in range(n):
        t0 = time.perf_counter()
        await fn()
        latencies_us.append((time.perf_counter() - t0) * 1_000_000)
    total = time.perf_counter() - start
    return _stats(latencies_us, total)


def _stats(latencies_us: list[float], total_s: float) -> LatencyStats:
    latencies_us.sort()
    n = len(latencies_us)
    return LatencyStats(
        n=n,
        p50_us=latencies_us[int(n * 0.50)],
        p95_us=latencies_us[min(int(n * 0.95), n - 1)],
        p99_us=latencies_us[min(int(n * 0.99), n - 1)],
        max_us=latencies_us[-1],
        total_s=total_s,
    )
