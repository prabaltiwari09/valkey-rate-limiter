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

import time
import math
import hashlib
from dataclasses import dataclass
from typing import Optional

try:
    import valkey
except ImportError:
    print("Install the client: pip install valkey")
    raise


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

-- Refill based on elapsed time
local elapsed_sec = (now_ms - last_refill) / 1000.0
local refill      = math.floor(elapsed_sec * refill_rate)
tokens = math.min(capacity, tokens + refill)

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


# ─── Main class ───────────────────────────────────────────────────────────────

class ValkeyRateLimiter:
    """
    Thread-safe, production-grade rate limiter backed by Valkey.

    Supports three strategies:
      - sliding_window : classic per-user sliding window (requests)
      - token_bucket   : smooth bursts, uses Valkey 9.0 hash field TTL
      - token_aware    : for LLM/AI APIs — limit by token count, not requests
    """

    def __init__(self, host: str = "localhost", port: int = 6379, db: int = 0):
        self.client = valkey.Valkey(host=host, port=port, db=db, decode_responses=True)
        self._sliding_script  = self.client.register_script(SLIDING_WINDOW_LUA)
        self._bucket_script   = self.client.register_script(TOKEN_BUCKET_LUA)

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

    print("  Normal request:", "ALLOWED" if safe_rate_limit("user_999") else "BLOCKED")
    print("  (To test fail-open: stop Valkey and re-run)")


# ─── Entry point ──────────────────────────────────────────────────────────────

if __name__ == "__main__":
    print("\n  Connecting to Valkey at localhost:6379 ...")
    try:
        limiter = ValkeyRateLimiter(host="localhost", port=6379)
        print("  ✓ Connected\n")
    except Exception as e:
        print(f"  ✗ Could not connect: {e}")
        print("  Start Valkey: docker run -p 6379:6379 valkey/valkey:latest")
        exit(1)

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
