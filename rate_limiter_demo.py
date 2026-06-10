"""
Valkey Rate Limiter — Live Demo
================================
Talk: "Rate Limiting at Scale: From Redis Patterns to Valkey — and Into the AI Era"
First Valkey Meetup Delhi | June 13, 2025

Implements three strategies:
  1. Sliding Window  — with Lua (atomic, no race conditions)
  2. Token Bucket    — uses Valkey 9.0 hash field TTL (HEXPIRE)
  3. Token-aware     — for AI/LLM APIs (limits by token count, not request count)

Requirements:
  pip install valkey

Run:
  # Start Valkey
  docker run -d \
  --name valkey-demo \
  -p 6379:6379 \
  valkey/valkey:latest

  # Run demo
  python rate_limiter_demo.py
"""

import sys
import time
import math
import hashlib
import threading
import random
import statistics
import argparse
from dataclasses import dataclass
from typing import Optional
from concurrent.futures import ThreadPoolExecutor

try:
    import valkey
except ImportError:
    print("Install the client: pip install valkey")
    raise


# ─── Stats Collector ──────────────────────────────────────────────────────────

class StatsCollector:
    def __init__(self, max_latency_samples: int = 100_000):
        self._lock = threading.Lock()
        self.max_latency_samples = max_latency_samples
        self.total_requests = 0
        self.allowed = 0
        self.blocked = 0
        self.errors = 0
        self.tokens_consumed = 0
        self.budget_exhausted_at: Optional[float] = None
        self.latencies: list[float] = []
        self.start_time = time.time()
        self._sample_count = 0  # total seen, for reservoir sampling

    def record(self, allowed: bool, latency_ms: float, tokens: int = 0) -> None:
        with self._lock:
            self.total_requests += 1
            if allowed:
                self.allowed += 1
            else:
                self.blocked += 1
            if tokens:
                self.tokens_consumed += tokens
                if not allowed and self.budget_exhausted_at is None:
                    self.budget_exhausted_at = time.time() - self.start_time
            self._sample_count += 1
            if len(self.latencies) < self.max_latency_samples:
                self.latencies.append(latency_ms)
            else:
                # Reservoir sampling: replace random element
                j = random.randint(0, self._sample_count - 1)
                if j < self.max_latency_samples:
                    self.latencies[j] = latency_ms

    def record_error(self) -> None:
        with self._lock:
            self.errors += 1

    def percentile(self, p: int) -> float:
        with self._lock:
            if not self.latencies:
                return 0.0
            snapshot = list(self.latencies)
        sorted_lat = sorted(snapshot)
        idx = max(0, int(len(sorted_lat) * p / 100) - 1)
        return sorted_lat[idx]

    def rps(self) -> float:
        with self._lock:
            total = self.total_requests
        elapsed = time.time() - self.start_time
        return total / elapsed if elapsed > 0 else 0.0

    def snapshot(self) -> dict:
        """Return a point-in-time copy of counters (for interval-delta reporting)."""
        with self._lock:
            return {
                "total": self.total_requests,
                "allowed": self.allowed,
                "blocked": self.blocked,
                "tokens": self.tokens_consumed,
                "t": time.time(),
            }


# ─── Data classes ─────────────────────────────────────────────────────────────

@dataclass
class RateLimitResult:
    allowed: bool
    count: int
    limit: int
    remaining: int
    reset_ms: int          # milliseconds until window resets (or bucket refills)
    strategy: str

    def __str__(self):
        status = "✓ ALLOWED" if self.allowed else "✗ BLOCKED"
        bar_filled = int((self.count / self.limit) * 20)
        bar = "█" * bar_filled + "░" * (20 - bar_filled)
        return (
            f"{status}  [{bar}]  "
            f"{self.count}/{self.limit}  "
            f"(resets in {self.reset_ms}ms)"
        )


# ─── Lua scripts ──────────────────────────────────────────────────────────────

SLIDING_WINDOW_LUA = """
local key     = KEYS[1]
local now     = tonumber(ARGV[1])
local window  = tonumber(ARGV[2])
local limit   = tonumber(ARGV[3])
local member  = ARGV[4]

-- Remove all entries outside the current window
redis.call('ZREMRANGEBYSCORE', key, 0, now - window)

-- Count how many requests are in the current window
local count = redis.call('ZCARD', key)

if count < limit then
    -- Add this request with its timestamp as both score and member
    redis.call('ZADD', key, now, member)
    redis.call('PEXPIRE', key, window)
    return {1, count + 1}   -- allowed=true, new count
end

return {0, count}            -- allowed=false, current count
"""

TOKEN_BUCKET_LUA = """
-- Token bucket using a Valkey hash.
-- Valkey 9.0+: uses HEXPIRE for per-field TTL (no key sprawl).
--
-- Hash fields:
--   tokens      — current token balance
--   last_refill — epoch ms of last refill
--
-- If the hash doesn't exist we bootstrap it.

local key           = KEYS[1]
local capacity      = tonumber(ARGV[1])
local refill_rate   = tonumber(ARGV[2])   -- tokens per second
local now_ms        = tonumber(ARGV[3])
local tokens_needed = tonumber(ARGV[4])
local ttl_seconds   = tonumber(ARGV[5])

local tokens     = tonumber(redis.call('HGET', key, 'tokens'))
local last_refill = tonumber(redis.call('HGET', key, 'last_refill'))

if not tokens then
    -- First request: bootstrap full bucket
    tokens      = capacity
    last_refill = now_ms
end

-- Refill based on elapsed time (float: no fractional-token loss at high RPS)
local elapsed_sec = (now_ms - last_refill) / 1000.0
tokens = math.min(capacity, tokens + elapsed_sec * refill_rate)

if tokens >= tokens_needed then
    tokens = tokens - tokens_needed
    redis.call('HSET', key, 'tokens', tokens, 'last_refill', now_ms)

    -- Valkey 9.0 HEXPIRE: per-field TTL without a separate key
    redis.call('HEXPIRE', key, ttl_seconds, 'FIELDS', 2, 'tokens', 'last_refill')

    return {1, tokens, capacity}   -- allowed, remaining, capacity
end

redis.call('HSET', key, 'tokens', tokens, 'last_refill', now_ms)
return {0, tokens, capacity}       -- blocked, remaining, capacity
"""


# ─── Scale worker ─────────────────────────────────────────────────────────────

_SCALE_LIMITS = {
    "sliding_window": {"limit": 50,  "window_ms": 1_000},      # 50 req/s per user → ~60% allowed
    "token_bucket":   {"capacity": 500, "refill_rate": 20.0},  # 500 burst → ~6s high-allow phase, then settles ~24% at 82 req/s/user
    "token_aware":    {"daily_token_budget": 80_000_000},       # 80M budget → exhausts ~18-20s at 14k RPS
}

_MAX_RETRIES = 3
_RETRY_DELAY = 0.01  # seconds


def scale_worker(
    user_id: str,
    limiter: "ValkeyRateLimiter",
    strategy: str,
    stats: StatsCollector,
    stop_event: threading.Event,
) -> None:
    while not stop_event.is_set():
        retries = 0
        while retries <= _MAX_RETRIES:
            t0 = time.perf_counter()
            tok = 0
            try:
                if strategy == "sliding_window":
                    result = limiter.sliding_window(
                        user_id, **_SCALE_LIMITS["sliding_window"]
                    )
                elif strategy == "token_bucket":
                    result = limiter.token_bucket(
                        user_id, **_SCALE_LIMITS["token_bucket"]
                    )
                elif strategy == "token_aware":
                    tok = random.randint(50, 500)
                    result = limiter.token_aware(
                        "org_shared",   # all users draw from one org-level budget
                        tokens_consumed=tok,
                        **_SCALE_LIMITS["token_aware"],
                    )
                else:
                    raise ValueError(f"Unknown strategy: {strategy!r}")
                latency_ms = (time.perf_counter() - t0) * 1000
                stats.record(allowed=result.allowed, latency_ms=latency_ms, tokens=tok)
                break
            except ConnectionError:
                retries += 1
                if retries > _MAX_RETRIES:
                    stats.record_error()
                else:
                    if stop_event.wait(_RETRY_DELAY):
                        return


# ─── Output helpers for demo_massive_scale ────────────────────────────────────

_LATENCY_BUCKETS = [
    (0,  1,  " 0- 1ms"),
    (1,  2,  " 1- 2ms"),
    (2,  5,  " 2- 5ms"),
    (5,  10, " 5-10ms"),
    (10, float("inf"), "10ms+ "),
]


def _print_histogram(stats: StatsCollector) -> None:
    with stats._lock:
        latencies = list(stats.latencies)
    if not latencies:
        print("  (no latency data)")
        return
    total = len(latencies)
    print()
    for lo, hi, label in _LATENCY_BUCKETS:
        count = sum(1 for v in latencies if lo <= v < hi)
        pct = count / total * 100
        bar = "█" * int(pct / 2)
        print(f"  {label}  {bar:<50} {pct:>4.0f}%")
    print()


def _print_math_explanation(stats: StatsCollector, strategy: str, num_users: int) -> None:
    rps = stats.rps()
    rps_per_user = rps / num_users if num_users else 0
    actual_pct = stats.allowed / stats.total_requests * 100 if stats.total_requests else 0
    print("  Why these numbers?")
    print("  " + "─" * 56)
    if strategy == "sliding_window":
        limit    = _SCALE_LIMITS["sliding_window"]["limit"]
        window_s = _SCALE_LIMITS["sliding_window"]["window_ms"] / 1000
        expected = min(100, limit / rps_per_user * 100) if rps_per_user else 0
        print(f"  Each of the {num_users} users fires ~{rps_per_user:.0f} req/s, but the limit is")
        print(f"  {limit} req per {window_s:.0f}s window — so only {limit}/{rps_per_user:.0f} = ~{expected:.0f}% can pass.")
        print(f"  Notice the per-second numbers are rock-steady at ~{actual_pct:.0f}%.")
        print(f"  That's the rolling window working: no fixed reset means no")
        print(f"  burst can sneak through at a boundary — the limit holds every")
        print(f"  single second, not just on average. Expected {expected:.0f}%, actual {actual_pct:.0f}%.")
    elif strategy == "token_bucket":
        capacity    = _SCALE_LIMITS["token_bucket"]["capacity"]
        refill_rate = _SCALE_LIMITS["token_bucket"]["refill_rate"]
        burst_s     = capacity / rps_per_user if rps_per_user else 0
        steady_pct  = min(100, refill_rate / rps_per_user * 100) if rps_per_user else 0
        print(f"  Each user starts with {capacity} tokens — enough to absorb ~{burst_s:.0f}s of")
        print(f"  full-speed traffic ({rps_per_user:.0f} req/s × {burst_s:.0f}s ≈ {capacity} tokens consumed).")
        print(f"  Once the bucket empties, only the refill rate matters:")
        print(f"  {refill_rate:.0f} tok/s ÷ {rps_per_user:.0f} req/s = ~{steady_pct:.0f}% allowed at steady state.")
        print(f"  The {actual_pct:.0f}% overall average is higher because the burst")
        print(f"  phase (100% allowed for ~{burst_s:.0f}s) pulls the whole-run average up.")
    elif strategy == "token_aware":
        budget = _SCALE_LIMITS["token_aware"]["daily_token_budget"]
        if stats.total_requests:
            avg_tok      = stats.tokens_consumed / stats.total_requests
            burn_rate    = avg_tok * rps
            exhausted_at = stats.budget_exhausted_at or 0
            predicted_s  = budget / burn_rate if burn_rate else 0
            print(f"  Requests averaged {avg_tok:.0f} tokens each. At {rps:,.0f} RPS that's")
            print(f"  {avg_tok:.0f} × {rps:,.0f} = ~{burn_rate:,.0f} tokens/s burning through the budget.")
            print(f"  {budget:,} ÷ {burn_rate:,.0f} tok/s = ~{predicted_s:.0f}s to exhaust.")
            print(f"  Actual exhaustion: {exhausted_at:.1f}s — request-based limiting would")
            print(f"  never have caught this; only token-aware limiting can.")
    print()


def _print_summary(
    stats: StatsCollector,
    strategy: str,
    num_users: int,
    duration: int,
) -> None:
    allowed_pct = (stats.allowed / stats.total_requests * 100) if stats.total_requests else 0
    blocked_pct = 100 - allowed_pct
    print("\n" + "═" * 50)
    print("  SCALE TEST RESULTS")
    print("═" * 50)
    print(f"  Strategy        : {strategy}")
    print(f"  Users           : {num_users:,}")
    print(f"  Duration        : {duration}s")
    print(f"  Total Requests  : {stats.total_requests:,}")
    print(f"  Throughput      : {stats.rps():,.0f} RPS")
    if strategy == "token_aware":
        budget = _SCALE_LIMITS["token_aware"]["daily_token_budget"]
        exhausted_at = stats.budget_exhausted_at
        print(f"  Tokens Consumed : {stats.tokens_consumed:,} / {budget:,}")
        if exhausted_at is not None:
            print(f"  Budget Exhausted: {exhausted_at:.1f}s into run")
        else:
            print(f"  Budget Exhausted: never (budget not fully consumed)")
    else:
        print(f"  Allowed         : {allowed_pct:.1f}%  ({stats.allowed:,})")
        print(f"  Blocked         : {blocked_pct:.1f}%  ({stats.blocked:,})")
    print(f"  Errors          : {stats.errors:,}")
    print()
    p50  = stats.percentile(50)
    p75  = stats.percentile(75)
    p95  = stats.percentile(95)
    p99  = stats.percentile(99)
    with stats._lock:
        latencies_snapshot = list(stats.latencies)
    pmax = max(latencies_snapshot) if latencies_snapshot else 0.0
    print(f"  Latency (ms)    p50    p75    p95    p99    max")
    print(f"                {p50:>5.1f}  {p75:>5.1f}  {p95:>5.1f}  {p99:>5.1f}  {pmax:>5.1f}")
    _print_histogram(stats)
    _print_math_explanation(stats, strategy, num_users)


_STRATEGY_DESCRIPTIONS = {
    "sliding_window": (
        "  Sliding Window\n"
        "  Each request is timestamped and stored in a sorted set.\n"
        "  On every call, entries older than the window are pruned,\n"
        "  then the count is checked against the limit — all atomically\n"
        "  via Lua. No fixed reset boundary: the window rolls with time.\n"
        "  Here: 50 req / 1 s window — a burst in the last 500ms still\n"
        "  counts against the next 500ms, so the limit holds continuously."
    ),
    "token_bucket": (
        "  Token Bucket\n"
        "  Each user has a bucket of tokens that refills at a fixed rate.\n"
        "  Requests consume tokens; when the bucket is empty, they're\n"
        "  blocked. Bursts are absorbed up to the bucket capacity.\n"
        "  Uses Valkey 9.0 HEXPIRE for per-field TTL on the hash —\n"
        "  one key per user instead of two, ~20% less memory."
    ),
    "token_aware": (
        "  Token-Aware (AI/LLM)\n"
        "  Limits by token count consumed, not request count.\n"
        "  A single 100K-token request costs 10× more than 100 requests\n"
        "  of 1K tokens each — request-based limits miss this entirely.\n"
        "  Uses INCR + daily expiry: simple, atomic, and cheap."
    ),
}


def _limit_summary(strategy: str) -> str:
    limits = _SCALE_LIMITS[strategy]
    if strategy == "sliding_window":
        return f"{limits['limit']} req / {limits['window_ms']}ms window"
    elif strategy == "token_bucket":
        return f"capacity={limits['capacity']}, refill={limits['refill_rate']} tok/s"
    elif strategy == "token_aware":
        return f"budget={limits['daily_token_budget']:,} tokens/day"
    return ""


def _print_strategy_description(strategy: str) -> None:
    desc = _STRATEGY_DESCRIPTIONS.get(strategy)
    if desc:
        print("─" * 50)
        print(desc)
        print("─" * 50)


def _print_progress(
    stats: StatsCollector,
    elapsed: int,
    strategy: str = "",
    prev: Optional[dict] = None,
) -> None:
    cur = stats.snapshot()
    if cur["total"] == 0:
        return
    p50 = stats.percentile(50)
    p99 = stats.percentile(99)

    # Use interval delta when a previous snapshot is available
    if prev is not None:
        dt = cur["t"] - prev["t"]
        interval_total   = cur["total"]   - prev["total"]
        interval_allowed = cur["allowed"] - prev["allowed"]
        interval_rps     = interval_total / dt if dt > 0 else 0.0
        allowed_pct      = interval_allowed / interval_total * 100 if interval_total else 0.0
    else:
        interval_rps = stats.rps()
        allowed_pct  = cur["allowed"] / cur["total"] * 100

    blocked_pct = 100 - allowed_pct

    if strategy == "token_aware":
        budget = _SCALE_LIMITS["token_aware"]["daily_token_budget"]
        consumed = cur["tokens"]
        pct = consumed / budget * 100
        exhausted = "  *** BUDGET EXHAUSTED ***" if consumed >= budget else ""
        print(
            f"  [{elapsed:>3}s] "
            f"RPS: {interval_rps:>7,.0f}  "
            f"Tokens: {consumed:>12,.0f} / {budget:,}  ({pct:.1f}%){exhausted}"
        )
    else:
        print(
            f"  [{elapsed:>3}s] "
            f"RPS: {interval_rps:>7,.0f}  "
            f"Allowed: {allowed_pct:.0f}%  "
            f"Blocked: {blocked_pct:.0f}%  "
            f"p50: {p50:.1f}ms  "
            f"p99: {p99:.1f}ms"
        )


def demo_massive_scale(
    limiter: "ValkeyRateLimiter",
    strategies: list[str],
    num_users: int = 200,
    duration: int = 30,
    progress_interval: int = 5,
) -> None:
    for strategy in strategies:
        print("\n" + "═" * 60)
        print(f"  SCALE DEMO: {strategy}  ({num_users:,} users, {duration}s)  [{_limit_summary(strategy)}]")
        print("═" * 60)
        _print_strategy_description(strategy)
        print()

        if strategy == "token_aware":
            limiter.client.delete(f"rl:ta:org_shared:{ValkeyRateLimiter._today_key()}")

        stats = StatsCollector()
        stop_event = threading.Event()

        # Progress reporter — 1s ticks for token_bucket/sliding_window to show transient behaviour
        _interval = 1 if strategy in ("token_bucket", "sliding_window") else progress_interval
        def _reporter(stats=stats, stop_event=stop_event, strategy=strategy, _interval=_interval):
            tick = 0
            prev = stats.snapshot()
            while tick < duration and not stop_event.wait(_interval):
                tick += _interval
                if not stop_event.is_set():
                    _print_progress(stats, tick, strategy, prev=prev)
                    prev = stats.snapshot()

        reporter = threading.Thread(target=_reporter, daemon=True)
        reporter.start()

        user_ids = [f"scale_user_{i}" for i in range(num_users)]
        futures = []
        executor = ThreadPoolExecutor(max_workers=num_users)
        try:
            futures = [
                executor.submit(
                    scale_worker,
                    user_ids[i % num_users],
                    limiter,
                    strategy,
                    stats,
                    stop_event,
                )
                for i in range(num_users)
            ]
            time.sleep(duration)
        except KeyboardInterrupt:
            print("\n  Interrupted — collecting partial results...")
        finally:
            stop_event.set()
            executor.shutdown(wait=False, cancel_futures=True)

        # Summarize unexpected worker exceptions (suppress per-worker spam)
        worker_errors = [
            f.exception() for f in futures
            if f.done() and not f.cancelled() and f.exception()
        ]
        if worker_errors:
            unique = {type(e).__name__ for e in worker_errors}
            print(f"  ⚠  {len(worker_errors)} worker(s) raised exceptions: {', '.join(unique)}")

        _print_summary(stats, strategy=strategy, num_users=num_users, duration=duration)


# ─── Main class ───────────────────────────────────────────────────────────────

class ValkeyRateLimiter:
    """
    Thread-safe, production-grade rate limiter backed by Valkey.

    Supports three strategies:
      - sliding_window : classic per-user sliding window (requests)
      - token_bucket   : smooth bursts, uses Valkey 9.0 hash field TTL
      - token_aware    : for LLM/AI APIs — limit by token count, not requests
    """

    def __init__(
        self,
        host: str = "localhost",
        port: int = 6379,
        db: int = 0,
        connection_pool: Optional[valkey.ConnectionPool] = None,
    ):
        if connection_pool is not None:
            self.client = valkey.Valkey(connection_pool=connection_pool, decode_responses=True)
        else:
            self.client = valkey.Valkey(host=host, port=port, db=db, decode_responses=True)
        self._sliding_script = self.client.register_script(SLIDING_WINDOW_LUA)
        self._bucket_script  = self.client.register_script(TOKEN_BUCKET_LUA)

        # Smoke-test connection
        self.client.ping()

    # ── Strategy 1: Sliding Window ────────────────────────────────────────────

    def sliding_window(
        self,
        user_id: str,
        limit: int,
        window_ms: int = 60_000,
    ) -> RateLimitResult:
        """
        Sliding window counter using a sorted set.

        Each request is stored as a member scored by its timestamp.
        Expired entries are pruned on every call (lazy expiry).
        Fully atomic via Lua — no race conditions.
        """
        now_ms = int(time.time() * 1000)
        key    = f"rl:sw:{user_id}"

        # Unique member to prevent ZADD dedup on same-millisecond requests
        member = f"{now_ms}:{hashlib.md5(f'{user_id}{now_ms}'.encode()).hexdigest()[:8]}"

        result = self._sliding_script(
            keys=[key],
            args=[now_ms, window_ms, limit, member],
        )

        allowed = bool(result[0])
        count   = int(result[1])
        return RateLimitResult(
            allowed=allowed,
            count=count,
            limit=limit,
            remaining=max(0, limit - count),
            reset_ms=window_ms,
            strategy="sliding_window",
        )

    # ── Strategy 2: Token Bucket ──────────────────────────────────────────────

    def token_bucket(
        self,
        user_id: str,
        capacity: int      = 10,
        refill_rate: float = 2.0,    # tokens/second
        tokens_needed: int = 1,
        ttl_seconds: int   = 3600,
    ) -> RateLimitResult:
        """
        Token bucket using a Valkey hash with Valkey 9.0 per-field TTL.

        Why hash + HEXPIRE (v9.0)?
          Before: 2 separate keys per user (tokens + last_refill) = key sprawl
          After:  1 hash, HEXPIRE sets TTL on individual fields = 20% less memory

        Allows controlled bursting up to `capacity`,
        refills continuously at `refill_rate` tokens/second.
        """
        now_ms = int(time.time() * 1000)
        key    = f"rl:tb:{user_id}"

        result = self._bucket_script(
            keys=[key],
            args=[capacity, refill_rate, now_ms, tokens_needed, ttl_seconds],
        )

        allowed   = bool(result[0])
        remaining = int(result[1])
        cap       = int(result[2])
        used      = cap - remaining

        return RateLimitResult(
            allowed=allowed,
            count=used,
            limit=cap,
            remaining=remaining,
            reset_ms=int((tokens_needed / refill_rate) * 1000),
            strategy="token_bucket",
        )

    # ── Strategy 3: Token-Aware (AI/LLM) ─────────────────────────────────────

    def token_aware(
        self,
        user_id: str,
        tokens_consumed: int,
        daily_token_budget: int = 100_000,
    ) -> RateLimitResult:
        """
        Rate limiting by token count — critical for LLM APIs.

        The problem with request-based limits for AI:
          - User A: 100 requests × 100 tokens  =  10,000 tokens (cheap)
          - User B: 1  request  × 100K tokens  = 100,000 tokens (10x expensive)
          - Same rate limit. Wildly different costs.

        This uses Valkey INCR + daily expiry.
        With Valkey 9.0, combine with token_bucket for hybrid protection:
          token_bucket   → protects against burst (req/min)
          token_aware    → protects against cost (tokens/day)
        """
        key    = f"rl:ta:{user_id}:{self._today_key()}"
        window = 86_400   # 24 hours in seconds

        pipe = self.client.pipeline()
        pipe.incrby(key, tokens_consumed)
        pipe.expire(key, window)
        results = pipe.execute()

        total_used = int(results[0])
        allowed    = total_used <= daily_token_budget

        # If this push went over, we still record it (idempotent audit trail)
        return RateLimitResult(
            allowed=allowed,
            count=total_used,
            limit=daily_token_budget,
            remaining=max(0, daily_token_budget - total_used),
            reset_ms=(window - int(time.time() % window)) * 1000,
            strategy="token_aware",
        )

    @staticmethod
    def _today_key() -> str:
        return time.strftime("%Y-%m-%d", time.gmtime())


# ─── Demo runners ─────────────────────────────────────────────────────────────

def demo_sliding_window(limiter: ValkeyRateLimiter):
    print("\n" + "═" * 60)
    print("  DEMO 1: Sliding Window  (5 req / 10 sec window)")
    print("═" * 60)
    for i in range(8):
        result = limiter.sliding_window("user_123", limit=5, window_ms=10_000)
        print(f"  Req {i+1:>2}:  {result}")
        time.sleep(0.4)

    print("\n  Waiting 10s for window to reset...")
    time.sleep(10)

    print("\n  Window reset — requests should be allowed again:")
    result = limiter.sliding_window("user_123", limit=5, window_ms=10_000)
    print(f"  Req 9:   {result}")


def demo_token_bucket(limiter: ValkeyRateLimiter):
    print("\n" + "═" * 60)
    print("  DEMO 2: Token Bucket  (capacity=5, refill=1 token/sec)")
    print("═" * 60)
    print("  — Burst 6 requests immediately:")
    for i in range(6):
        result = limiter.token_bucket("user_456", capacity=5, refill_rate=1.0)
        print(f"  Req {i+1:>2}:  {result}")

    print("\n  Waiting 3 seconds (bucket refills 3 tokens)...")
    time.sleep(3)
    print("  — 3 more requests (should allow 3):")
    for i in range(3):
        result = limiter.token_bucket("user_456", capacity=5, refill_rate=1.0)
        print(f"  Req {i+7:>2}:  {result}")


def demo_token_aware(limiter: ValkeyRateLimiter):
    print("\n" + "═" * 60)
    print("  DEMO 3: Token-Aware  (daily budget = 1,000 tokens)")
    print("═" * 60)

    requests = [
        ("GPT-4o small call",   150),
        ("GPT-4o medium call",  300),
        ("GPT-4o large call",   400),
        ("GPT-4o huge call",    800),   # This should push over budget
    ]
    for name, tokens in requests:
        result = limiter.token_aware("user_789", tokens_consumed=tokens, daily_token_budget=1_000)
        print(f"  {name:<25} ({tokens:>5} tokens):  {result}")


def demo_failure_mode(limiter: ValkeyRateLimiter):
    """Shows fail-open pattern when Valkey is unavailable."""
    print("\n" + "═" * 60)
    print("  DEMO 4: Failure Mode — Fail Open")
    print("═" * 60)

    def safe_rate_limit(user_id: str) -> bool:
        try:
            result = limiter.sliding_window(user_id, limit=5, window_ms=10_000)
            return result.allowed
        except Exception as e:
            # Fail open: log and allow when Valkey is unreachable
            print(f"  ⚠  Valkey unreachable ({type(e).__name__}). Failing open.")
            return True

    print("  [Phase 1] Valkey healthy:")
    for i in range(1, 4):
        outcome = "ALLOWED" if safe_rate_limit("user_999") else "BLOCKED"
        print(f"    Request {i}: {outcome}")

    # Simulate Valkey going down by replacing execute with a failing stub
    original_execute = limiter.client.execute_command

    def broken_execute(*a, **kw):  # noqa: ANN002
        raise ConnectionError("Simulated Valkey outage")

    limiter.client.execute_command = broken_execute

    print("\n  [Phase 2] Valkey goes down — fail-open kicks in:")
    for i in range(1, 4):
        outcome = "ALLOWED" if safe_rate_limit("user_999") else "BLOCKED"
        print(f"    Request {i}: {outcome}")

    # Restore so subsequent demos aren't affected
    limiter.client.execute_command = original_execute

    print("\n  [Phase 3] Valkey recovers:")
    for i in range(1, 4):
        outcome = "ALLOWED" if safe_rate_limit("user_999") else "BLOCKED"
        print(f"    Request {i}: {outcome}")


# ─── Entry point ──────────────────────────────────────────────────────────────

def build_arg_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Valkey Rate Limiter Demo"
    )
    parser.add_argument(
        "--scale",
        action="store_true",
        help="Run scale simulation (200 users, ~30s)",
    )
    parser.add_argument(
        "--strategy",
        choices=["sliding_window", "token_bucket", "token_aware", "all"],
        default="all",
        help="Strategy to test in scale mode (default: all)",
    )
    return parser


if __name__ == "__main__":
    args = build_arg_parser().parse_args()

    if args.strategy != "all" and not args.scale:
        print("Warning: --strategy has no effect without --scale")

    print("\n  Connecting to Valkey at localhost:6379 ...")
    try:
        if args.scale:
            pool = valkey.ConnectionPool(
                host="localhost", port=6379, db=0,
                max_connections=250,
            )
            limiter = ValkeyRateLimiter(connection_pool=pool)
        else:
            limiter = ValkeyRateLimiter(host="localhost", port=6379)
        print("  ✓ Connected\n")
    except Exception as e:
        print(f"  ✗ Could not connect: {e}")
        print("  Start Valkey: docker run -p 6379:6379 valkey/valkey:latest")
        sys.exit(1)

    if args.scale:
        strategies = (
            ["sliding_window", "token_bucket", "token_aware"]
            if args.strategy == "all"
            else [args.strategy]
        )
        demo_massive_scale(limiter, strategies=strategies)
    else:
        # Clean up any leftover keys from a previous run
        limiter.client.delete(
            "rl:sw:user_123",
            "rl:tb:user_456",
            f"rl:ta:user_789:{ValkeyRateLimiter._today_key()}",
        )

        demo_sliding_window(limiter)
        demo_token_bucket(limiter)
        demo_token_aware(limiter)
        print("Waiting 5 seconds before failure mode demo...")
        time.sleep(5)
        demo_failure_mode(limiter)

        print("\n" + "═" * 60)
        print("  Demo complete. Source: github.com/prabal/valkey-rate-limiter")
        print("═" * 60 + "\n")
