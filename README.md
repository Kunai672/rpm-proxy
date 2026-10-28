# rpm-proxy

A local HTTP proxy for Nvidia integrate API requests with rate limiting, retries, timeout handling, and fallback model switching.

## Run

```bash
node rpm-proxy.mjs
```

## Options

- `--port` / `RPM_PROXY_PORT` (default 8787)
- `--upstream` / `RPM_PROXY_UPSTREAM`
- `--limit` / `RPM_PROXY_LIMIT` (default 8)
- `--window-secs` / `RPM_PROXY_WINDOW_SECS` (default 60)
- `--stagger-ms` / `RPM_PROXY_STAGGER_MS` (default 3000)
- `--timeout-delay` / `RPM_PROXY_TIMEOUT_DELAY` (default 10000)
- `--timeout-limit` / `RPM_PROXY_TIMEOUT_LIMIT` (default 1)
- `--fallback-model` / `RPM_PROXY_FALLBACK_MODEL`
- `--fallback-requests` / `RPM_PROXY_FALLBACK_REQUESTS` (default 10)
- `--quiet` / `RPM_PROXY_QUIET`

## Endpoints

- `GET /__rpm/health`
- `GET /__rpm/status`
