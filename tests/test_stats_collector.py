import sys
import os
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from unittest.mock import MagicMock, patch
import threading
import time
import valkey


def test_init_uses_connection_pool_when_provided():
    pool = MagicMock(spec=valkey.ConnectionPool)
    with patch("valkey.Valkey") as mock_valkey:
        mock_client = MagicMock()
        mock_client.ping.return_value = True
        mock_valkey.return_value = mock_client

        from rate_limiter_demo import ValkeyRateLimiter
        limiter = ValkeyRateLimiter(connection_pool=pool)

        mock_valkey.assert_called_once_with(connection_pool=pool, decode_responses=True)


def test_record_increments_counters():
    from rate_limiter_demo import StatsCollector
    stats = StatsCollector()
    stats.record(allowed=True, latency_ms=1.0)
    stats.record(allowed=False, latency_ms=2.0)
    assert stats.total_requests == 2
    assert stats.allowed == 1
    assert stats.blocked == 1
    assert stats.errors == 0


def test_record_error():
    from rate_limiter_demo import StatsCollector
    stats = StatsCollector()
    stats.record_error()
    assert stats.errors == 1
    assert stats.total_requests == 0


def test_reservoir_sampling_caps_at_max():
    from rate_limiter_demo import StatsCollector
    stats = StatsCollector(max_latency_samples=10)
    for i in range(50):
        stats.record(allowed=True, latency_ms=float(i))
    assert len(stats.latencies) <= 10


def test_percentile_returns_correct_value():
    from rate_limiter_demo import StatsCollector
    stats = StatsCollector()
    for i in range(1, 101):
        stats.record(allowed=True, latency_ms=float(i))
    assert stats.percentile(50) == 50.0
    assert stats.percentile(99) == 99.0


def test_thread_safety():
    from rate_limiter_demo import StatsCollector
    stats = StatsCollector()
    def worker():
        for _ in range(100):
            stats.record(allowed=True, latency_ms=0.5)
    threads = [threading.Thread(target=worker) for _ in range(20)]
    for t in threads:
        t.start()
    for t in threads:
        t.join()
    assert stats.total_requests == 2000
    assert stats.allowed == 2000


def _make_result(allowed: bool):
    from rate_limiter_demo import RateLimitResult
    return RateLimitResult(
        allowed=allowed, count=1, limit=10, remaining=9, reset_ms=1000,
        strategy="sliding_window"
    )


def test_scale_worker_records_allowed():
    from rate_limiter_demo import scale_worker, StatsCollector
    stop = threading.Event()
    stats = StatsCollector()
    limiter = MagicMock()
    limiter.sliding_window.return_value = _make_result(True)

    def run():
        scale_worker("user_0", limiter, "sliding_window", stats, stop)

    t = threading.Thread(target=run)
    t.start()
    time.sleep(0.05)
    stop.set()
    t.join(timeout=1)

    assert stats.total_requests > 0
    assert stats.allowed == stats.total_requests
    assert stats.errors == 0


def test_scale_worker_records_blocked():
    from rate_limiter_demo import scale_worker, StatsCollector
    stop = threading.Event()
    stats = StatsCollector()
    limiter = MagicMock()
    limiter.sliding_window.return_value = _make_result(False)

    def run():
        scale_worker("user_0", limiter, "sliding_window", stats, stop)

    t = threading.Thread(target=run)
    t.start()
    time.sleep(0.05)
    stop.set()
    t.join(timeout=1)

    assert stats.blocked == stats.total_requests


def test_scale_worker_handles_connection_error():
    from rate_limiter_demo import scale_worker, StatsCollector
    stop = threading.Event()
    stats = StatsCollector()
    limiter = MagicMock()
    limiter.sliding_window.side_effect = ConnectionError("Simulated")

    def run():
        scale_worker("user_0", limiter, "sliding_window", stats, stop)

    t = threading.Thread(target=run)
    t.start()
    time.sleep(0.05)
    stop.set()
    t.join(timeout=2)

    assert stats.errors > 0
    assert stats.total_requests == 0


def test_print_histogram_outputs_buckets(capsys):
    from rate_limiter_demo import _print_histogram, StatsCollector
    stats = StatsCollector()
    for v in [0.5, 0.8, 1.2, 3.0, 6.0]:
        stats.record(allowed=True, latency_ms=v)
    _print_histogram(stats)
    captured = capsys.readouterr()
    assert "0- 1ms" in captured.out
    assert "1- 2ms" in captured.out
    assert "5-10ms" in captured.out


def test_print_summary_outputs_key_fields(capsys):
    from rate_limiter_demo import _print_summary, StatsCollector
    stats = StatsCollector()
    for _ in range(7):
        stats.record(allowed=True, latency_ms=1.0)
    for _ in range(3):
        stats.record(allowed=False, latency_ms=2.0)
    _print_summary(stats, strategy="sliding_window", num_users=1000, duration=60)
    captured = capsys.readouterr()
    assert "sliding_window" in captured.out
    assert "1,000" in captured.out
    assert "70.0%" in captured.out
