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

---

## Quickstart

**1. Clone and set up the environment:**

```bash
git clone https://github.com/prabal/valkey-rate-limiter.git
cd valkey-rate-limiter
bash setup_valkey.sh
source .venv/bin/activate
```

`setup_valkey.sh` creates a virtual environment and installs the only dependency (`valkey`).

**2. Start Valkey:**

```bash
docker run -d \
  --name valkey-demo \
  -p 6379:6379 \
  valkey/valkey:latest
```

**3. Run the demo:**

```bash
# Interactive demo (4 strategies, ~30s total)
python rate_limiter_demo.py

# Scale simulation (200 concurrent users, ~90s total)
python rate_limiter_demo.py --scale

# Scale simulation for a single strategy
python rate_limiter_demo.py --scale --strategy sliding_window
# choices: sliding_window | token_bucket | token_aware | all
```

**4. Run tests** (no Valkey needed — all mocked):

```bash
pytest tests/
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

## Sample output

```
  Connecting to Valkey at localhost:6379 ...
  ✓ Connected

════════════════════════════════════════════════════════════
  DEMO 1: Sliding Window  (5 req / 10 sec window)
════════════════════════════════════════════════════════════
  Req  1:  ✓ ALLOWED  [████░░░░░░░░░░░░░░░░]  1/5  (resets in 10000ms)
  Req  2:  ✓ ALLOWED  [████████░░░░░░░░░░░░]  2/5  (resets in 10000ms)
  Req  3:  ✓ ALLOWED  [████████████░░░░░░░░]  3/5  (resets in 10000ms)
  Req  4:  ✓ ALLOWED  [████████████████░░░░]  4/5  (resets in 10000ms)
  Req  5:  ✓ ALLOWED  [████████████████████]  5/5  (resets in 10000ms)
  Req  6:  ✗ BLOCKED  [████████████████████]  5/5  (resets in 10000ms)
  Req  7:  ✗ BLOCKED  [████████████████████]  5/5  (resets in 10000ms)
  Req  8:  ✗ BLOCKED  [████████████████████]  5/5  (resets in 10000ms)

  Waiting 10s for window to reset...

  Window reset — requests should be allowed again:
  Req 9:   ✓ ALLOWED  [████░░░░░░░░░░░░░░░░]  1/5  (resets in 10000ms)

════════════════════════════════════════════════════════════
  DEMO 2: Token Bucket  (capacity=5, refill=1 token/sec)
════════════════════════════════════════════════════════════
  — Burst 6 requests immediately:
  Req  1:  ✓ ALLOWED  [████░░░░░░░░░░░░░░░░]  1/5  (resets in 1000ms)
  Req  2:  ✓ ALLOWED  [████████░░░░░░░░░░░░]  2/5  (resets in 1000ms)
  Req  3:  ✓ ALLOWED  [████████████░░░░░░░░]  3/5  (resets in 1000ms)
  Req  4:  ✓ ALLOWED  [████████████████░░░░]  4/5  (resets in 1000ms)
  Req  5:  ✓ ALLOWED  [████████████████████]  5/5  (resets in 1000ms)
  Req  6:  ✗ BLOCKED  [████████████████████]  5/5  (resets in 1000ms)

  Waiting 3 seconds (bucket refills 3 tokens)...
  — 3 more requests (should allow 3):
  Req  7:  ✓ ALLOWED  [████████████░░░░░░░░]  3/5  (resets in 1000ms)
  Req  8:  ✓ ALLOWED  [████████████████░░░░]  4/5  (resets in 1000ms)
  Req  9:  ✓ ALLOWED  [████████████████████]  5/5  (resets in 1000ms)

════════════════════════════════════════════════════════════
  DEMO 3: Token-Aware  (daily budget = 1,000 tokens)
════════════════════════════════════════════════════════════
  GPT-4o small call         (  150 tokens):  ✓ ALLOWED  [███░░░░░░░░░░░░░░░░░]  150/1000
  GPT-4o medium call        (  300 tokens):  ✓ ALLOWED  [█████████░░░░░░░░░░░]  450/1000
  GPT-4o large call         (  400 tokens):  ✓ ALLOWED  [█████████████████░░░]  850/1000
  GPT-4o huge call          (  800 tokens):  ✗ BLOCKED  [█████████████████████████████████]  1650/1000

════════════════════════════════════════════════════════════
  DEMO 4: Failure Mode — Fail Open
════════════════════════════════════════════════════════════
  [Phase 1] Valkey healthy:
    Request 1: ALLOWED
    Request 2: ALLOWED
    Request 3: ALLOWED

  [Phase 2] Valkey goes down — fail-open kicks in:
  ⚠  Valkey unreachable (ConnectionError). Failing open.
    Request 1: ALLOWED
  ⚠  Valkey unreachable (ConnectionError). Failing open.
    Request 2: ALLOWED
  ⚠  Valkey unreachable (ConnectionError). Failing open.
    Request 3: ALLOWED

  [Phase 3] Valkey recovers:
    Request 1: ALLOWED
    Request 2: ALLOWED
    Request 3: BLOCKED
```

---

## Key design choices

- **All mutations are atomic Lua scripts** — no TOCTOU races under concurrent load
- **Unique sorted set members** — MD5 suffix prevents ZADD deduplication on same-millisecond requests
- **Fail-open on exceptions** — connection errors allow traffic through rather than hard-blocking users
- **Pipeline for token-aware** — `INCRBY` + `EXPIRE` in one round trip
