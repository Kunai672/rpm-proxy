#!/usr/bin/env node
/**
 * rpm-proxy.mjs — rate-limiting reverse proxy for OpenAI-compatible APIs (NVIDIA NIM).
 *
 * Every pi instance (host, containers, VMs) points its baseUrl here, so ONE
 * process owns the request budget and sees every request, including ones the
 * pi extension can't (compaction, sub-agents, other tools, other machines).
 *
 *   node rpm-proxy.mjs --upstream https://integrate.api.nvidia.com --limit 30
 *   # then in pi, set the provider's baseUrl to http://127.0.0.1:8787/v1
 *
 * Behavior
 *   - POST requests wait for a slot (sliding window, FIFO, bursts up to --limit).
 *   - Other methods (GET /v1/models …) pass through uncounted.
 *   - Upstream 429 → shared cooldown (Retry-After, else exponential backoff) and
 *     the request is transparently re-queued, up to --max-retries, so pi never
 *     sees the 429. If retries run out, the last 429 is passed through.
 *   - Client disconnects (Esc, pi timeout) cancel the queued reservation and
 *     abort the upstream request.
 *   - Streaming (SSE) is piped through unbuffered. The Authorization header is
 *     forwarded and never logged or stored.
 *   - GET /__rpm/status → JSON for the pi footer; GET /__rpm/health → "ok".
 *
 * Flags (env in parentheses):
 *   --upstream URL   (RPM_PROXY_UPSTREAM) default https://integrate.api.nvidia.com
 *   --limit N        (RPM_PROXY_LIMIT)    default 30   (keep below the real quota)
 *   --window SECS    (RPM_PROXY_WINDOW)   default 60
 *   --pad MS         (RPM_PROXY_PAD)      default 250
 *   --port N         (RPM_PROXY_PORT)     default 8787
 *   --host ADDR      (RPM_PROXY_HOST)     default 127.0.0.1  (use 0.0.0.0 for containers)
 *   --max-retries N  (RPM_PROXY_RETRIES)  default 5   (429 retries per request)
 *   --retry-base MS  (RPM_PROXY_RETRY_BASE) default 5000 (backoff when no Retry-After)
 *   --stagger MS     (RPM_PROXY_STAGGER)  default 2000 (extra delay per POST after rate limit)
 *   --quiet                               suppress per-request log lines
 */

import http from "node:http";
import https from "node:https";
import { pathToFileURL } from "node:url";

// ───────────────────────────── limiter ─────────────────────────────

const sleep = (ms, signal) =>
  new Promise((resolve, reject) => {
    if (signal?.aborted) return reject(abortError());
    const onAbort = () => {
      clearTimeout(t);
      reject(abortError());
    };
    const t = setTimeout(() => {
      signal?.removeEventListener("abort", onAbort);
      resolve();
    }, ms);
    signal?.addEventListener("abort", onAbort, { once: true });
  });

function abortError() {
  const e = new Error("aborted");
  e.name = "AbortError";
  return e;
}

export class Limiter {
  constructor({ limit, windowMs, padMs }) {
    this.limit = limit;
    this.windowMs = windowMs;
    this.padMs = padMs;
    this.slots = []; // sorted send times (ms), including future reservations
    this.cooldownUntil = 0;
  }

  _prune(now) {
    const cutoff = now - this.windowMs - this.padMs;
    while (this.slots.length && this.slots[0] <= cutoff) this.slots.shift();
  }

  _nextTime(now) {
    const s = this.slots;
    let t = Math.max(now, this.cooldownUntil);
    if (s.length) t = Math.max(t, s[s.length - 1]); // FIFO
    if (s.length >= this.limit) t = Math.max(t, s[s.length - this.limit] + this.windowMs + this.padMs);
    return t;
  }

  _remove(t) {
    const i = this.slots.indexOf(t);
    if (i >= 0) this.slots.splice(i, 1);
  }

  /** Resolves when the caller may send. Rejects with AbortError (reservation released). */
  async acquire(signal) {
    const started = Date.now();
    for (;;) {
      const now = Date.now();
      this._prune(now);
      const t = this._nextTime(now);
      this.slots.push(t);
      if (t > now) {
        try {
          await sleep(t - now, signal);
        } catch (e) {
          this._remove(t);
          throw e;
        }
      }
      if (this.cooldownUntil > Date.now()) {
        // a 429 landed while we slept; our slot is stale → queue again behind it
        this._remove(t);
        continue;
      }
      return { waitedMs: Date.now() - started };
    }
  }

  cooldown(ms) {
    this.cooldownUntil = Math.max(this.cooldownUntil, Date.now() + ms);
  }

  snapshot() {
    const now = Date.now();
    this._prune(now);
    const used = this.slots.filter((x) => x <= now && x > now - this.windowMs).length;
    return {
      limit: this.limit,
      windowSecs: this.windowMs / 1000,
      used,
      available: this.limit - used,
      queued: this.slots.filter((x) => x > now).length,
      nextSlotInMs: Math.max(0, this._nextTime(now) - now),
      cooldownMs: Math.max(0, this.cooldownUntil - now),
      utilizationPct: Math.round((used / this.limit) * 100),
    };
  }
}

// ───────────────────────────── helpers ─────────────────────────────

const HOP = new Set([
  "connection", "keep-alive", "proxy-authenticate", "proxy-authorization",
  "te", "trailer", "transfer-encoding", "upgrade", "host", "content-length",
]);

function parseRetryAfterMs(headers) {
  const raw = headers["retry-after"];
  if (raw == null) return undefined;
  const str = String(raw).trim();
  const secs = Number(str);
  if (Number.isFinite(secs)) return Math.max(0, secs * 1000);
  const d = Date.parse(str);
  return Number.isNaN(d) ? undefined : Math.max(0, d - Date.now());
}

function readBody(req, maxBytes) {
  return new Promise((resolve, reject) => {
    const chunks = [];
    let size = 0;
    req.on("data", (c) => {
      size += c.length;
      if (size > maxBytes) {
        reject(Object.assign(new Error("body too large"), { code: "E2BIG" }));
        req.destroy();
        return;
      }
      chunks.push(c);
    });
    req.on("end", () => resolve(Buffer.concat(chunks)));
    req.on("error", reject);
    req.on("aborted", () => reject(abortError()));
  });
}

function sendJson(res, status, obj) {
  if (res.headersSent) return res.destroy();
  const body = JSON.stringify(obj);
  res.writeHead(status, { "content-type": "application/json", "content-length": Buffer.byteLength(body) });
  res.end(body);
}

// ───────────────────────────── proxy ─────────────────────────────

export function startProxy(opts = {}) {
  const cfg = {
    upstream: "https://integrate.api.nvidia.com",
    limit: 30,
    windowSecs: 60,
    padMs: 250,
    port: 8787,
    host: "127.0.0.1",
    maxRetries: 5,
    retryBaseMs: 5000,
    retryCapMs: 60_000,
    maxBodyBytes: 32 * 1024 * 1024,
    quiet: false,
    staggerMs: 2000,
    ...opts,
  };
  const base = cfg.upstream.replace(/\/+$/, "");
  const limiter = new Limiter({ limit: cfg.limit, windowMs: cfg.windowSecs * 1000, padMs: cfg.padMs });
  const stats = { requests: 0, retried429: 0, passed429: 0, aborted: 0, errors: 0, inflight: 0, startedAt: Date.now() };
  const agents = { "https:": new https.Agent({ keepAlive: true }), "http:": new http.Agent({ keepAlive: true }) };
  const log = (msg) => !cfg.quiet && console.log(`${new Date().toISOString()} ${msg}`);

  function forward(method, pathAndQuery, headers, body, signal) {
    return new Promise((resolve, reject) => {
      const u = new URL(base + pathAndQuery);
      const h = {};
      for (const [k, v] of Object.entries(headers)) if (!HOP.has(k.toLowerCase())) h[k] = v;
      h.host = u.host;
      if (body.length || method === "POST" || method === "PUT" || method === "PATCH") h["content-length"] = String(body.length);
      const lib = u.protocol === "https:" ? https : http;
      const r = lib.request(u, { method, headers: h, agent: agents[u.protocol] }, resolve);
      r.on("error", reject);
      const onAbort = () => r.destroy(abortError());
      if (signal.aborted) return onAbort();
      signal.addEventListener("abort", onAbort, { once: true });
      r.on("close", () => signal.removeEventListener("abort", onAbort));
      r.end(body);
    });
  }

  async function handle(req, res) {
    const url = req.url ?? "/";
    if (url.startsWith("/__rpm/status")) {
      return sendJson(res, 200, { ...limiter.snapshot(), inflight: stats.inflight, totals: stats, uptimeSecs: Math.round((Date.now() - stats.startedAt) / 1000) });
    }
    if (url.startsWith("/__rpm/health")) {
      res.writeHead(200, { "content-type": "text/plain" });
      return res.end("ok");
    }

    const ac = new AbortController();
    res.on("close", () => {
      if (!res.writableFinished) ac.abort(); // client went away (Esc, timeout, kill)
    });

    let body;
    try {
      body = await readBody(req, cfg.maxBodyBytes);
    } catch (e) {
      if (e.code === "E2BIG") return sendJson(res, 413, { error: { message: "request body too large", type: "rpm_proxy" } });
      return; // client aborted mid-upload
    }

    const t0 = Date.now();
    const counted = req.method === "POST";
    const tag = `${req.method} ${url.split("?")[0]}`;
    stats.requests++;
    stats.inflight++;
    let waited = 0;
    let attempts = 0;

    try {
      for (let attempt = 0; ; attempt++) {
        attempts = attempt + 1;
        if (counted) {
          const snapBefore = limiter.snapshot();
          const acq = await limiter.acquire(ac.signal);
          waited += acq.waitedMs;
          if (acq.waitedMs > 0) {
            log(`${tag} waited ${acq.waitedMs}ms for slot | window: ${snapBefore.used}/${snapBefore.limit} (${snapBefore.utilizationPct}%) | avail: ${snapBefore.available} | queued: ${snapBefore.queued}`);
          }
          if (cfg.staggerMs > 0) {
            await sleep(cfg.staggerMs, ac.signal);
            waited += cfg.staggerMs;
          }
        }

        const up = await forward(req.method, url, req.headers, body, ac.signal);

        if (counted && up.statusCode === 429 && attempt < cfg.maxRetries) {
          const ra = parseRetryAfterMs(up.headers);
          const backoff = Math.min(cfg.retryCapMs, cfg.retryBaseMs * 2 ** attempt);
          const ms = ra ?? backoff;
          up.resume(); // drain and discard the 429 body
          limiter.cooldown(ms);
          stats.retried429++;
          const snap429 = limiter.snapshot();
          log(`${tag} upstream 429 → cooldown ${Math.round(ms / 1000)}s${ra == null ? " (no Retry-After)" : ""}, retry ${attempt + 1}/${cfg.maxRetries} | window: ${snap429.used}/${snap429.limit} (${snap429.utilizationPct}%) | avail: ${snap429.available} | queued: ${snap429.queued}`);
          continue;
        }
        if (up.statusCode === 429) {
          stats.passed429++;
          if (counted) {
            const snap429 = limiter.snapshot();
            limiter.cooldown(parseRetryAfterMs(up.headers) ?? cfg.retryBaseMs);
            log(`${tag} 429 after ${cfg.maxRetries} retries — passing through | window: ${snap429.used}/${snap429.limit} (${snap429.utilizationPct}%) | avail: ${snap429.available} | queued: ${snap429.queued}`);
          }
        }

        const h = {};
        for (const [k, v] of Object.entries(up.headers)) if (!HOP.has(k.toLowerCase())) h[k] = v;
        h["x-rpm-proxy-wait-ms"] = String(waited);
        h["x-rpm-proxy-attempts"] = String(attempts);
        res.writeHead(up.statusCode ?? 502, h);
        res.flushHeaders();
        res.on("close", () => up.destroy());
        up.on("error", () => res.destroy());
        up.pipe(res);
        const snap = limiter.snapshot();
        log(`${tag} ${up.statusCode} wait=${waited}ms${counted && cfg.staggerMs > 0 ? ` stagger=${cfg.staggerMs}ms` : ""} attempts=${attempts} ttfb=${Date.now() - t0 - waited}ms | window: ${snap.used}/${snap.limit} (${snap.utilizationPct}%) | avail: ${snap.available} | queued: ${snap.queued}`);
        return;
      }
    } catch (e) {
      if (e?.name === "AbortError" || ac.signal.aborted) {
        stats.aborted++;
        log(`${tag} client gone after ${Date.now() - t0}ms (wait=${waited}ms) — released`);
        return;
      }
      stats.errors++;
      log(`${tag} upstream error: ${e?.code ?? e?.message}`);
      sendJson(res, 502, { error: { message: `rpm-proxy: upstream error (${e?.code ?? e?.message})`, type: "rpm_proxy_upstream" } });
    } finally {
      stats.inflight--;
    }
  }

  const server = http.createServer((req, res) => {
    handle(req, res).catch((e) => {
      stats.errors++;
      log(`internal error: ${e?.message}`);
      sendJson(res, 500, { error: { message: "rpm-proxy internal error", type: "rpm_proxy" } });
    });
  });
  server.requestTimeout = 0;
  server.keepAliveTimeout = 60_000;

  return new Promise((resolve) =>
    server.listen(cfg.port, cfg.host, () => {
      const port = server.address().port;
      log(`rpm-proxy listening on http://${cfg.host}:${port} → ${base} (limit ${cfg.limit}/${cfg.windowSecs}s, retries ${cfg.maxRetries})`);

      // Periodic status logging every 10 seconds
      const statusInterval = setInterval(() => {
        const snap = limiter.snapshot();
        const rate = stats.requests / ((Date.now() - stats.startedAt) / 1000 / 60);
        log(`[STATUS] window: ${snap.used}/${snap.limit} (${snap.utilizationPct}%) | available: ${snap.available} | queued: ${snap.queued} | cooldown: ${Math.round(snap.cooldownMs / 1000)}s | next slot: ${Math.round(snap.nextSlotInMs / 1000)}s | inflight: ${stats.inflight} | total req/min: ${rate.toFixed(1)} | retried 429: ${stats.retried429} | passed 429: ${stats.passed429} | aborted: ${stats.aborted} | errors: ${stats.errors}`);
      }, 10_000);
      statusInterval.unref();

      resolve({ server, limiter, stats, port, close: () => new Promise((r) => { clearInterval(statusInterval); server.closeAllConnections?.(); server.close(() => r()); }) });
    }),
  );
}

// ───────────────────────────── CLI ─────────────────────────────

function parseArgs(argv) {
  const out = {};
  for (let i = 0; i < argv.length; i++) {
    const a = argv[i];
    if (!a.startsWith("--")) continue;
    const key = a.slice(2);
    out[key] = argv[i + 1] && !argv[i + 1].startsWith("--") ? argv[++i] : true;
  }
  return out;
}

if (process.argv[1] && import.meta.url === pathToFileURL(process.argv[1]).href) {
  const a = parseArgs(process.argv.slice(2));
  const e = process.env;
  const num = (v, d) => (v === undefined || v === true || Number.isNaN(Number(v)) ? d : Number(v));
  const proxy = await startProxy({
    upstream: a.upstream ?? e.RPM_PROXY_UPSTREAM ?? "https://integrate.api.nvidia.com",
    limit: num(a.limit ?? e.RPM_PROXY_LIMIT, 30),
    windowSecs: num(a.window ?? e.RPM_PROXY_WINDOW, 60),
    padMs: num(a.pad ?? e.RPM_PROXY_PAD, 250),
    port: num(a.port ?? e.RPM_PROXY_PORT, 8787),
    host: a.host ?? e.RPM_PROXY_HOST ?? "127.0.0.1",
    maxRetries: num(a["max-retries"] ?? e.RPM_PROXY_RETRIES, 5),
    retryBaseMs: num(a["retry-base"] ?? e.RPM_PROXY_RETRY_BASE, 5000),
    staggerMs: num(a.stagger ?? e.RPM_PROXY_STAGGER, 2000),
    quiet: !!a.quiet,
  });
  const stop = async () => {
    await proxy.close();
    process.exit(0);
  };
  process.on("SIGINT", stop);
  process.on("SIGTERM", stop);
}
