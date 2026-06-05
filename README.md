# Valkey Rate Limiter

Live demo code for the talk **"Rate Limiting at Scale: From Redis Patterns to Valkey — and Into the AI Era"**  
First Valkey Meetup Delhi · June 13, 2025

Three production-grade rate limiting strategies implemented in pure Python + Lua, backed by [Valkey](https://valkey.io).

---

## Strategies

| # | Strategy | Use case | Key feature |
|---|----------|----------|-------------|
| 1 | **Sliding Window** | General API rate limiting | Atomic Lua, no race conditions |
| 2 | **Token Bucket** | Bursty traffic with smooth refill | Valkey 9.0 `HEXPIRE` (per-field TTL) |
| 3 | **Token-Aware** | LLM / AI APIs | Limits by token count, not request count |
| 4 | **Failure Mode** | Resilience demo | Fail-open pattern with simulated outage |

---

## Requirements

- Python 3.8+
- Valkey 9.0+ (for `HEXPIRE` support in the token bucket demo)

```bash
pip install valkey
```

---

## Quickstart

**Start Valkey:**

```bash
docker run -d \
  --name valkey-demo \
  -p 6379:6379 \
  valkey/valkey:latest
```

**Run the demo:**

```bash
python rate_limiter_demo.py
```

---

## What each demo shows

### Demo 1 — Sliding Window
- 5 requests allowed per 10-second window
- Fires 8 rapid requests → first 5 allowed, rest blocked
- Waits for window reset → requests allowed again

### Demo 2 — Token Bucket
- Capacity of 5 tokens, refills at 1 token/sec
- Bursts 6 requests immediately → 6th blocked
- Waits 3 seconds → 3 tokens refill, next 3 allowed

### Demo 3 — Token-Aware (AI/LLM)
- Daily budget of 1,000 tokens
- Simulates four LLM calls of increasing size
- Final call pushes over budget → blocked

> **Why token-aware matters:** Two users hitting the same request-based limit can consume wildly different compute. A single 100K-token request costs 10× more than 100 requests at 100 tokens each.

### Demo 4 — Failure Mode (Fail Open)
- Phase 1: normal requests while Valkey is healthy
- Phase 2: Valkey outage simulated inline — requests still allowed (fail-open)
- Phase 3: Valkey "recovers" — normal operation resumes

---

## Valkey 9.0 highlight: `HEXPIRE`

The token bucket stores both `tokens` and `last_refill` in a single hash. With `HEXPIRE`, each field gets its own TTL — no separate expiry key, no key sprawl, ~20% less memory vs. the two-key Redis pattern.

```lua
redis.call('HEXPIRE', key, ttl_seconds, 'FIELDS', 2, 'tokens', 'last_refill')
```

---

## Key design choices

- **All mutations are atomic Lua scripts** — no TOCTOU races under concurrent load
- **Unique sorted set members** — MD5 suffix prevents ZADD deduplication on same-millisecond requests
- **Fail-open on exceptions** — connection errors allow traffic through rather than hard-blocking users
- **Pipeline for token-aware** — `INCRBY` + `EXPIRE` in one round trip
