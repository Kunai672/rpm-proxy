# rpm-proxy

A local HTTP **rate-limiting reverse proxy** for the NVIDIA NIM integrate API
(an OpenAI-compatible endpoint). It sits between pi and upstream so that a single
process owns the request budget, keeping you under quota and hiding upstream
failures.

Every pi instance (host, containers, VMs) points its baseUrl here, so **one**
process sees every request — including ones the pi extension can't (compaction,
sub-agents, other tools, other machines).

## Why

- **Stays under upstream quota.** Counted requests are gated through a sliding
  window, so you never exceed the rate the real API allows.
- **Silently recovers from 429s.** Upstream `429`s are retried with backoff; pi
  never sees the status.
- **Recovers from timeouts.** On `ETIMEDOUT` it backs off and retries; persistent
  timeouts switch to a fallback model.
- **Shares one budget across machines** when pointed at from multiple instances.

## Run

```bash
node rpm-proxy.mjs --upstream https://integrate.api.nvidia.com --limit 8
```

It listens on `http://127.0.0.1:8787` by default.

### Point pi at it

In your provider settings, set the **baseUrl** to:

```
http://127.0.0.1:8787/v1
```

All requests pi makes are rewritten to flow through the proxy first, then forwarded
to the real upstream.

## Options

Each option can be passed as a CLI flag **or** an environment variable.

| Flag | Env var | Default | Description |
| --- | --- | --- | --- |
| `--port` | `RPM_PROXY_PORT` | `8787` | Listen port |
| `--host` | `RPM_PROXY_HOST` | `127.0.0.1` | Listen address (use `0.0.0.0` for containers/remote) |
| `--upstream` | `RPM_PROXY_UPSTREAM` | `https://integrate.api.nvidia.com` | Where to forward requests |
| `--limit` | `RPM_PROXY_LIMIT` | `8` | Max counted requests per window — keep below the real quota |
| `--window-secs` | `RPM_PROXY_WINDOW` | `60` | Sliding-window length (seconds) |
| `--pad` | `RPM_PROXY_PAD` | `250` | Extra margin (ms) on each window |
| `--stagger-ms` | `RPM_PROXY_STAGGER` | `3000` | Gap between counted upstream requests (ms) |
| `--max-retries` | `RPM_PROXY_RETRIES` | `5` | 429 retries per request |
| `--retry-base` | `RPM_PROXY_RETRY_BASE` | `5000` | Backoff base for 429 (ms) |
| `--limit429-delay` | `RPM_PROXY_429_DELAY` | `10000` | Delay all queued requests after a 429 (ms) |
| `--timeout-delay` | `RPM_PROXY_TIMEOUT_DELAY` | `10000` | Delay all queued requests on timeout (ms) |
| `--timeout-limit` | `RPM_PROXY_TIMEOUT_LIMIT` | `1` | Timeouts before switching to fallback model |
| `--fallback-model` | `RPM_PROXY_FALLBACK_MODEL` | `nvidia/nemotron-3-ultra-550b-a55b` | Model used on exhaustion/timeouts |
| `--fallback-requests` | `RPM_PROXY_FALLBACK_REQUESTS` | `10` | Requests after fallback is triggered |
| `--quiet` | — | off | Suppress per-request log lines |
| `--color` | `RPM_PROXY_COLOR` | auto (TTY) | Force color on/off |

Example:

```bash
node rpm-proxy.mjs \
  --upstream https://integrate.api.nvidia.com \
  --limit 8 \
  --fallback-model nvidia/nemotron-3-ultra-550b-a55b \
  --fallback-requests 10 \
  --quiet
```

## Behavior

- **Counted requests (POST)** wait for a FIFO sliding-window slot, enter a pool,
  and are dispatched one at a time — the next POST starts only after the previous
  response completes **plus** the `--stagger-ms` delay.
- **Other methods (GET /v1/models, etc.)** pass through uncalled-counted.
- **Upstream 429** → shared cooldown (`Retry-After`, else exponential backoff) and
  the request is transparently re-queued, up to `--max-retries`, so pi never sees
  the 429. If retries run out, the last 429 is passed through.
- **Client disconnects** (Esc, pi timeout, kill) cancel the queued reservation and
  abort the upstream request.
- **Streaming (SSE)** is piped through unbuffered. The `Authorization` header is
  forwarded and never logged or stored.

## Debug endpoints

```bash
curl http://127.0.0.1:8787/__rpm/health   # → "ok"
curl http://127.0.0.1:8787/__rpm/status    # → JSON runtime state
```

`/__rpm/status` returns limiter/runtime state:

```json
{
  "limit": 8,
  "windowSecs": 60,
  "used": 3,
  "available": 5,
  "queued": 0,
  "utilizationPct": 37,
  "nextSlotInMs": 120,
  "cooldownMs": 0,
  "globalDelayMs": 0,
  "serializing": false,
  "maxConcurrentUpstream": 8,
  "inflight": 1,
  "totals": {
    "requests": 42,
    "retried429": 3,
    "passed429": 0,
    "aborted": 0,
    "errors": 0
  },
  "uptimeSecs": 150
}
```

## Quick check

```bash
node rpm-proxy.mjs &          # start in background
sleep 1
curl -s http://127.0.0.1:8787/__rpm/health   # → ok
kill %1                      # stop
```

## Notes

- **Keep `--limit` below your real upstream quota** — the goal is to stay under it.
  Set it too high and you'll still get 429s.
- **For containers/remote hosts**, use `--host 0.0.0.0` and point your baseUrl at
  that machine's IP and port instead of `127.0.0.1`.
- Log lines are prefixed with the request ID so you can follow a single request
  through `REQUEST → WAIT → RESPONSE`, `429`, or `TIMEOUT` transitions.

## Tests

```bash
node rpm-proxy.test.mjs
```
