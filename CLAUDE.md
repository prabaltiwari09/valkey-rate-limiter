# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project overview

Talk demo for "Rate Limiting at Scale: From Redis Patterns to Valkey — and Into the AI Era." Single-file Python project (`rate_limiter_demo.py`) with no framework — all logic lives in that one module.

## Commands

```bash
# Setup (first time)
bash setup_valkey.sh
source .venv/bin/activate

# Start Valkey (required before running anything)
docker run -d --name valkey-demo -p 6379:6379 valkey/valkey:latest

# Run interactive demo
python rate_limiter_demo.py

# Run scale simulation (200 users, ~90s total)
python rate_limiter_demo.py --scale
python rate_limiter_demo.py --scale --strategy sliding_window

# Run tests (no Valkey needed — all mocked)
pytest tests/
pytest tests/test_stats_collector.py::test_thread_safety   # single test
```

## Architecture

Everything is in `rate_limiter_demo.py`. Key components:

- **`ValkeyRateLimiter`** — main class, three strategies:
  - `sliding_window()` — sorted set + Lua script (`SLIDING_WINDOW_LUA`), atomic, no TOCTOU
  - `token_bucket()` — hash + Lua script (`TOKEN_BUCKET_LUA`), uses Valkey 9.0 `HEXPIRE` for per-field TTL
  - `token_aware()` — `INCRBY` + `EXPIRE` pipeline, limits by token count not request count
- **`StatsCollector`** — thread-safe counters with reservoir sampling for latency (p50/p75/p95/p99)
- **`scale_worker()`** — tight loop calling a limiter strategy, feeds into `StatsCollector`
- **`demo_massive_scale()`** — spawns `ThreadPoolExecutor` with one worker per user, runs for `duration` seconds, prints interval progress + final histogram

Key prefixes for Valkey keys: `rl:sw:` (sliding window), `rl:tb:` (token bucket), `rl:ta:` (token-aware).

## Design constraints

- All mutations are Lua scripts — never split them into multi-step Python
- `HEXPIRE` requires Valkey 9.0+; token bucket will fail on older versions
- Sorted set members use an MD5 suffix to prevent ZADD deduplication on same-millisecond requests
- Fail-open on `ConnectionError` is intentional — don't change to fail-closed without discussion

---

# context-mode — MANDATORY routing rules

You have context-mode MCP tools available. These rules are NOT optional — they protect your context window from flooding. A single unrouted command can dump 56 KB into context and waste the entire session.

## BLOCKED commands — do NOT attempt these

### curl / wget — BLOCKED
Any Bash command containing `curl` or `wget` is intercepted and replaced with an error message. Do NOT retry.
Instead use:
- `ctx_fetch_and_index(url, source)` to fetch and index web pages
- `ctx_execute(language: "javascript", code: "const r = await fetch(...)")` to run HTTP calls in sandbox

### Inline HTTP — BLOCKED
Any Bash command containing `fetch('http`, `requests.get(`, `requests.post(`, `http.get(`, or `http.request(` is intercepted and replaced with an error message. Do NOT retry with Bash.
Instead use:
- `ctx_execute(language, code)` to run HTTP calls in sandbox — only stdout enters context

### WebFetch — BLOCKED
WebFetch calls are denied entirely. The URL is extracted and you are told to use `ctx_fetch_and_index` instead.
Instead use:
- `ctx_fetch_and_index(url, source)` then `ctx_search(queries)` to query the indexed content

## REDIRECTED tools — use sandbox equivalents

### Bash (>20 lines output)
Bash is ONLY for: `git`, `mkdir`, `rm`, `mv`, `cd`, `ls`, `npm install`, `pip install`, and other short-output commands.
For everything else, use:
- `ctx_batch_execute(commands, queries)` — run multiple commands + search in ONE call
- `ctx_execute(language: "shell", code: "...")` — run in sandbox, only stdout enters context

### Read (for analysis)
If you are reading a file to **Edit** it → Read is correct (Edit needs content in context).
If you are reading to **analyze, explore, or summarize** → use `ctx_execute_file(path, language, code)` instead. Only your printed summary enters context. The raw file content stays in the sandbox.

### Grep (large results)
Grep results can flood context. Use `ctx_execute(language: "shell", code: "grep ...")` to run searches in sandbox. Only your printed summary enters context.

## Tool selection hierarchy

1. **GATHER**: `ctx_batch_execute(commands, queries)` — Primary tool. Runs all commands, auto-indexes output, returns search results. ONE call replaces 30+ individual calls.
2. **FOLLOW-UP**: `ctx_search(queries: ["q1", "q2", ...])` — Query indexed content. Pass ALL questions as array in ONE call.
3. **PROCESSING**: `ctx_execute(language, code)` | `ctx_execute_file(path, language, code)` — Sandbox execution. Only stdout enters context.
4. **WEB**: `ctx_fetch_and_index(url, source)` then `ctx_search(queries)` — Fetch, chunk, index, query. Raw HTML never enters context.
5. **INDEX**: `ctx_index(content, source)` — Store content in FTS5 knowledge base for later search.

## Subagent routing

When spawning subagents (Agent/Task tool), the routing block is automatically injected into their prompt. Bash-type subagents are upgraded to general-purpose so they have access to MCP tools. You do NOT need to manually instruct subagents about context-mode.

## Output constraints

- Keep responses under 500 words.
- Write artifacts (code, configs, PRDs) to FILES — never return them as inline text. Return only: file path + 1-line description.
- When indexing content, use descriptive source labels so others can `ctx_search(source: "label")` later.

## ctx commands

| Command | Action |
|---------|--------|
| `ctx stats` | Call the `ctx_stats` MCP tool and display the full output verbatim |
| `ctx doctor` | Call the `ctx_doctor` MCP tool, run the returned shell command, display as checklist |
| `ctx upgrade` | Call the `ctx_upgrade` MCP tool, run the returned shell command, display as checklist |
