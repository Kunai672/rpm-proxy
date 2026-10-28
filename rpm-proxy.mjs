#!/usr/bin/env node
/**
 * rpm-proxy.mjs — rate-limiting reverse proxy for OpenAI-compatible APIs (NVIDIA NIM).
 *
 * Every pi instance (host, containers, VMs) points its baseUrl here, so ONE
 * process owns the request budget and sees every request, including ones the
 * pi extension can't (compaction, sub-agents, other tools, other machines).
 *
 *   node rpm-proxy.mjs --upstream https://integrate.api.nvidia.com --limit 8
 *   # then in pi, set the provider's baseUrl to http://127.0.0.1:8787/v1
 *
 * Behavior
 *   - POST requests wait for a FIFO sliding-window slot and enter the request pool.
 *   - Counted upstream requests are dispatched one at a time. The next POST starts
 *     only after the previous response completes plus the --stagger delay.
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
 *   --upstream URL      (RPM_PROXY_UPSTREAM)      default https://integrate.api.nvidia.com
 *   --limit N           (RPM_PROXY_LIMIT)         default 8   (keep below the real quota)
 *   --window SECS       (RPM_PROXY_WINDOW)        default 60
 *   --pad MS            (RPM_PROXY_PAD)           default 250
 *   --port N            (RPM_PROXY_PORT)         default 8787
 *   --host ADDR         (RPM_PROXY_HOST)         default 127.0.0.1  (use 0.0.0.0 for containers)
 *   --max-retries N     (RPM_PROXY_RETRIES)       default 5   (429 retries per request)
 *   --retry-base MS     (RPM_PROXY_RETRY_BASE)    default 5000 (backoff when no Retry-After)
 *   --stagger MS        (RPM_PROXY_STAGGER)       default 3000 (gap between counted upstream requests)
 *   --limit429-delay MS (RPM_PROXY_429_DELAY)     default 10000 (delay all queued requests on 429)
 *   --timeout-delay MS  (RPM_PROXY_TIMEOUT_DELAY) default 30000 (delay all queued requests on ETIMEDOUT)
 *   --quiet                                      suppress per-request log lines
 *   --color                 (RPM_PROXY_COLOR)     force color output (auto: TTY)
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

/**
 * FIFO gate for counted upstream requests.
 *
 * A caller owns the gate until it releases it. Releasing waits for the stagger
 * interval before the next queued caller is admitted, so upstream requests never
 * overlap and are separated by at least the configured delay.
 */
class SequentialGate {
  constructor(staggerMs) {
    this.staggerMs = Number.isFinite(staggerMs) ? Math.max(0, staggerMs) : 0;
    this.waiters = [];
    this.active = false;
    this.releaseTimer = null;
  }

  acquire(signal) {
    if (signal?.aborted) return Promise.reject(abortError());
    const started = Date.now();

    return new Promise((resolve, reject) => {
      const waiter = { started, resolve, reject, signal, onAbort: null };
      waiter.onAbort = () => {
        const index = this.waiters.indexOf(waiter);
        if (index >= 0) this.waiters.splice(index, 1);
        this._removeAbortListener(waiter);
        reject(abortError());
      };

      signal?.addEventListener("abort", waiter.onAbort, { once: true });
      this.waiters.push(waiter);
      this._drain();
    });
  }

  _removeAbortListener(waiter) {
    if (waiter.signal && waiter.onAbort) {
      waiter.signal.removeEventListener("abort", waiter.onAbort);
    }
  }

  _drain() {
    if (this.active || this.waiters.length === 0) return;

    this.active = true;
    const waiter = this.waiters.shift();
    this._removeAbortListener(waiter);

    let released = false;
    const release = () => {
      if (released) return;
      released = true;

      const open = () => {
        this.releaseTimer = null;
        this.active = false;
        this._drain();
      };

      if (this.staggerMs > 0) {
        this.releaseTimer = setTimeout(open, this.staggerMs);
        this.releaseTimer.unref?.();
      } else {
        open();
      }
    };

    waiter.resolve({ waitedMs: Date.now() - waiter.started, release });
  }
}

export class Limiter {
  constructor({ limit, windowMs, padMs, staggerMs, limit429DelayMs = 10000, timeoutDelayMs = 30000 }) {
    this.limit = limit;
    this.windowMs = windowMs;
    this.padMs = padMs;
    this.staggerMs = staggerMs;
    this.limit429DelayMs = limit429DelayMs;
    this.timeoutDelayMs = timeoutDelayMs;
    this.slots = []; // sorted send times (ms), including future reservations
    this.cooldownUntil = 0;
    this.globalDelayUntil = 0;
    this.sequentialGate = new SequentialGate(staggerMs);
  }

  _prune(now) {
    const cutoff = now - this.windowMs - this.padMs;
    while (this.slots.length && this.slots[0] <= cutoff) this.slots.shift();
  }

  _nextTime(now) {
    const s = this.slots;
    let t = Math.max(now, this.cooldownUntil, this.globalDelayUntil);
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
      if (this.cooldownUntil > Date.now() || this.globalDelayUntil > Date.now()) {
        // a 429 landed while we slept; our slot is stale → queue again behind it
        this._remove(t);
        continue;
      }
      return { waitedMs: Date.now() - started };
    }
  }

  /**
   * Acquire the sequential gate for a counted request.
   * The returned release function must be called when the upstream attempt ends.
   */
  async acquireSequential(signal) {
    return this.sequentialGate.acquire(signal);
  }

  cooldown(ms) {
    this.cooldownUntil = Math.max(this.cooldownUntil, Date.now() + ms);
  }

  delayAll(ms) {
    this.globalDelayUntil = Math.max(this.globalDelayUntil, Date.now() + ms);
  }

  snapshot() {
    const now = Date.now();
    this._prune(now);
    const used = this.slots.filter((x) => x <= now && x > now - this.windowMs).length;
    const rateSlotQueued = this.slots.filter((x) => x > now).length;
    const sequentialQueued = this.sequentialGate.waiters.length;
    const available = this.limit - used;
    const cooldownMs = Math.max(0, this.cooldownUntil - now);
    const globalDelayMs = Math.max(0, this.globalDelayUntil - now);
    const serializing = available <= 1 || cooldownMs > 0 || globalDelayMs > 0;
    return {
      limit: this.limit,
      windowSecs: this.windowMs / 1000,
      used,
      available,
      queued: rateSlotQueued + sequentialQueued,
      rateSlotQueued,
      sequentialQueued,
      nextSlotInMs: Math.max(0, this._nextTime(now) - now),
      cooldownMs,
      globalDelayMs,
      utilizationPct: Math.round((used / this.limit) * 100),
      staggerMs: this.sequentialGate.staggerMs,
      serializing,
      maxConcurrentUpstream: serializing ? 1 : this.limit,
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
    limit: 8,
    windowSecs: 60,
    padMs: 250,
    port: 8787,
    host: "127.0.0.1",
    maxRetries: 5,
    retryBaseMs: 5000,
    retryCapMs: 60_000,
    maxBodyBytes: 32 * 1024 * 1024,
    quiet: false,
    staggerMs: 3000,
    limit429DelayMs: 10000,
    timeoutDelayMs: 30000,
    ...opts,
  };
  const base = cfg.upstream.replace(/\/+$/, "");
  const limiter = new Limiter({ limit: cfg.limit, windowMs: cfg.windowSecs * 1000, padMs: cfg.padMs, staggerMs: cfg.staggerMs, limit429DelayMs: cfg.limit429DelayMs, timeoutDelayMs: cfg.timeoutDelayMs });
  const stats = { requests: 0, retried429: 0, passed429: 0, aborted: 0, errors: 0, inflight: 0, startedAt: Date.now() };
  const agents = { "https:": new https.Agent({ keepAlive: true }), "http:": new http.Agent({ keepAlive: true }) };
  // Color support detection
  const useColor = cfg.color ?? (process.stdout.isTTY && !process.env.NO_COLOR);

  // ANSI color codes
  const C = {
    reset: useColor ? "\x1b[0m" : "",
    dim: useColor ? "\x1b[2m" : "",
    bold: useColor ? "\x1b[1m" : "",
    red: useColor ? "\x1b[31m" : "",
    green: useColor ? "\x1b[32m" : "",
    yellow: useColor ? "\x1b[33m" : "",
    blue: useColor ? "\x1b[34m" : "",
    magenta: useColor ? "\x1b[35m" : "",
    cyan: useColor ? "\x1b[36m" : "",
    gray: useColor ? "\x1b[90m" : "",
  };

  // Human-readable timestamp in fixed UTC+8: "Oct 28, 2026 10:00:00am"
  function formatTime(ts = Date.now()) {
    const d = new Date(ts + 8 * 60 * 60 * 1000);
    const month = d.toLocaleString("en-US", { month: "short", timeZone: "UTC" });
    const day = d.getUTCDate();
    const year = d.getUTCFullYear();
    let hour = d.getUTCHours();
    const minute = String(d.getUTCMinutes()).padStart(2, "0");
    const second = String(d.getUTCSeconds()).padStart(2, "0");
    const ampm = hour >= 12 ? "pm" : "am";
    hour = hour % 12 || 12;
    return `${month} ${day}, ${year} ${hour}:${minute}:${second}${ampm}`;
  }

  // Extract model from request/response body
  function extractModel(body) {
    if (!body || body.length === 0) return null;
    try {
      const parsed = JSON.parse(body.toString());
      return parsed.model ?? null;
    } catch {
      return null;
    }
  }

  const log = (msg) => !cfg.quiet && console.log(`${C.dim}${formatTime()}${C.reset} ${msg}`);

  function forward(method, pathAndQuery, headers, body, signal) {
    return new Promise((resolve, reject) => {
      const u = new URL(base + pathAndQuery);
      const h = {};
      for (const [k, v] of Object.entries(headers)) if (!HOP.has(k.toLowerCase())) h[k] = v;
      h.host = u.host;
      if (body.length || method === "POST" || method === "PUT" || method === "PATCH") h["content-length"] = String(body.length);
      const lib = u.protocol === "https:" ? https : http;
      const r = lib.request(u, { method, headers: h, agent: agents[u.protocol] }, resolve);
      r.on("error", (err) => {
        if (err?.code === "ETIMEDOUT") {
          reject(Object.assign(err, { code: "ETIMEDOUT" }));
        } else {
          reject(err);
        }
      });
      r.setTimeout?.(120000, () => {
        const err = new Error("timeout");
        err.code = "ETIMEDOUT";
        r.destroy(err);
      });
      const onAbort = () => r.destroy(abortError());
      if (signal.aborted) return onAbort();
      signal.addEventListener("abort", onAbort, { once: true });
      r.on("close", () => signal.removeEventListener("abort", onAbort));
      r.end(body);
    });
  }

  function drainResponse(response) {
    if (response.readableEnded || response.destroyed) return Promise.resolve();

    return new Promise((resolve) => {
      let done = false;
      const finish = () => {
        if (done) return;
        done = true;
        resolve();
      };
      response.once("end", finish);
      response.once("close", finish);
      response.once("error", finish);
      response.once("aborted", finish);
      response.resume();
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

    // Extract model from request body
    const reqModel = extractModel(body);

    const t0 = Date.now();
    const counted = req.method === "POST";
    const tag = `${req.method} ${url.split("?")[0]}`;
    stats.requests++;
    stats.inflight++;
    let waited = 0;
    let attempts = 0;
    let releaseSequential = null;

    // Log request received
    log(`${C.cyan}${req.method}${C.reset} ${C.bold}${url.split("?")[0]}${C.reset}${reqModel ? ` ${C.magenta}model=${reqModel}${C.reset}` : ""}`);

    try {
      for (let attempt = 0; ; attempt++) {
        attempts = attempt + 1;
        if (counted) {
          // Pool/queue all counted requests and stagger send each one up to the limit
          const sequential = await limiter.acquireSequential(ac.signal);
          releaseSequential = sequential.release;
          waited += sequential.waitedMs;
          if (sequential.waitedMs > 0) {
            const snapGate = limiter.snapshot();
            log(`${C.cyan}${tag}${C.reset} ${C.yellow}wait${C.reset}=${C.bold}${sequential.waitedMs}ms${C.reset} for sequential turn | window: ${snapGate.used}/${snapGate.limit} (${snapGate.utilizationPct}%) | avail: ${snapGate.available} | queued: ${snapGate.queued}`);
          }

          const acq = await limiter.acquire(ac.signal);
          waited += acq.waitedMs;
          if (acq.waitedMs > 0) {
            const snapNow = limiter.snapshot();
            log(`${C.cyan}${tag}${C.reset} ${C.yellow}wait${C.reset}=${C.bold}${acq.waitedMs}ms${C.reset} for slot | window: ${snapNow.used}/${snapNow.limit} (${snapNow.utilizationPct}%) | avail: ${snapNow.available} | queued: ${snapNow.queued}`);
          }
        }

        let up;
        try {
          up = await forward(req.method, url, req.headers, body, ac.signal);
        } catch (e) {
          if (e?.name === "AbortError" || ac.signal.aborted) throw e;
          if (counted && (e?.code === "ETIMEDOUT" || e?.message?.includes("timeout"))) {
            limiter.delayAll(cfg.timeoutDelayMs);
            stats.errors++;
            log(`${C.cyan}${tag}${C.reset} ${C.red}ETIMEDOUT${C.reset} → delaying all queued by ${C.bold}${Math.round(cfg.timeoutDelayMs / 1000)}s${C.reset}, retrying (not exposed) | attempt ${attempt + 1}`);
            releaseSequential?.();
            releaseSequential = null;
            await sleep(cfg.timeoutDelayMs, ac.signal);
            continue;
          }
          throw e;
        }

        if (counted && up.statusCode === 429 && attempt < cfg.maxRetries) {
          const ra = parseRetryAfterMs(up.headers);
          const backoff = Math.min(cfg.retryCapMs, cfg.retryBaseMs * 2 ** attempt);
          const ms = ra ?? backoff;
          limiter.cooldown(ms);
          limiter.delayAll(cfg.limit429DelayMs);
          stats.retried429++;
          const snap429 = limiter.snapshot();
          log(`${C.cyan}${tag}${C.reset} ${C.red}429${C.reset} → global delay ${C.bold}${Math.round(cfg.limit429DelayMs / 1000)}s${C.reset} + cooldown ${C.bold}${Math.round(ms / 1000)}s${C.reset}${ra == null ? ` ${C.gray}(no Retry-After)${C.reset}` : ""}, retry ${attempt + 1}/${cfg.maxRetries} | window: ${snap429.used}/${snap429.limit} (${snap429.utilizationPct}%) | avail: ${snap429.available} | queued: ${snap429.queued}`);
          await drainResponse(up);
          releaseSequential?.();
          releaseSequential = null;
          continue;
        }
        if (up.statusCode === 429) {
          stats.passed429++;
          if (counted) {
            const snap429 = limiter.snapshot();
            limiter.cooldown(parseRetryAfterMs(up.headers) ?? cfg.retryBaseMs);
            limiter.delayAll(cfg.limit429DelayMs);
            log(`${C.cyan}${tag}${C.reset} ${C.red}429${C.reset} after ${cfg.maxRetries} retries — passing through, delaying all queued by ${C.bold}${Math.round(cfg.limit429DelayMs / 1000)}s${C.reset} | window: ${snap429.used}/${snap429.limit} (${snap429.utilizationPct}%) | avail: ${snap429.available} | queued: ${snap429.queued}`);
          }
        }

        const h = {};
        for (const [k, v] of Object.entries(up.headers)) if (!HOP.has(k.toLowerCase())) h[k] = v;
        h["x-rpm-proxy-wait-ms"] = String(waited);
        h["x-rpm-proxy-attempts"] = String(attempts);
        try {
          res.writeHead(up.statusCode ?? 502, h);
          res.flushHeaders();
        } catch (e) {
          up.destroy();
          throw e;
        }
        res.on("close", () => up.destroy());
        up.on("error", () => res.destroy());

        if (releaseSequential) {
          const release = releaseSequential;
          releaseSequential = null;
          const releaseOnce = () => release();
          up.once("end", releaseOnce);
          up.once("close", releaseOnce);
          up.once("error", releaseOnce);
          up.once("aborted", releaseOnce);
          if (up.readableEnded || up.destroyed) releaseOnce();
        }

        const snap = limiter.snapshot();
        const statusColor = up.statusCode >= 400 ? C.red : up.statusCode >= 300 ? C.yellow : C.green;
        const ttfb = Date.now() - t0 - waited;
        let resModel = null;
        let responseChunks = [];
        let responseEnded = false;

        // Extract model from response body (non-streaming: buffer; streaming: first chunk)
        const contentType = up.headers["content-type"] || "";
        const isStreaming = contentType.includes("text/event-stream");

        const modelExtractor = (chunk) => {
          if (resModel) return;
          try {
            const text = chunk.toString();
            if (isStreaming) {
              // SSE format: data: {...}\n\n
              for (const line of text.split("\n")) {
                const trimmed = line.trim();
                if (trimmed.startsWith("data: ")) {
                  const data = trimmed.slice(6).trim();
                  if (data === "[DONE]") continue;
                  const parsed = JSON.parse(data);
                  if (parsed.model) { resModel = parsed.model; break; }
                }
              }
            } else {
              responseChunks.push(chunk);
            }
          } catch {}
        };

        up.on("data", modelExtractor);

        const logResponse = () => {
          if (responseEnded) return;
          responseEnded = true;
          const totalTime = Date.now() - t0;
          if (!isStreaming && !resModel) {
            try {
              const fullBody = Buffer.concat(responseChunks).toString();
              const parsed = JSON.parse(fullBody);
              resModel = parsed.model ?? null;
            } catch {}
          }
          const modelStr = resModel ? ` ${C.magenta}model=${resModel}${C.reset}` : "";
          log(`${C.cyan}${tag}${C.reset} ${statusColor}${up.statusCode}${C.reset} wait=${C.bold}${waited}ms${C.reset}${counted && cfg.staggerMs > 0 ? ` stagger=${cfg.staggerMs}ms` : ""} attempts=${attempts} ${C.blue}ttfb${C.reset}=${C.bold}${ttfb}ms${C.reset} ${C.blue}total${C.reset}=${C.bold}${totalTime}ms${C.reset}${modelStr} | window: ${snap.used}/${snap.limit} (${snap.utilizationPct}%) | avail: ${snap.available} | queued: ${snap.queued}`);
        };

        up.on("end", logResponse);
        res.on("finish", logResponse);

        up.pipe(res);
        return;
      }
    } catch (e) {
      if (e?.name === "AbortError" || ac.signal.aborted) {
        stats.aborted++;
        log(`${C.cyan}${tag}${C.reset} ${C.gray}client gone${C.reset} after ${Date.now() - t0}ms (wait=${waited}ms) — released`);
        return;
      }
      stats.errors++;
      log(`${C.cyan}${tag}${C.reset} ${C.red}error${C.reset}: ${e?.code ?? e?.message}`);
      sendJson(res, 502, { error: { message: `rpm-proxy: upstream error (${e?.code ?? e?.message})`, type: "rpm_proxy_upstream" } });
    } finally {
      releaseSequential?.();
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

      resolve({ server, limiter, stats, port, close: () => new Promise((r) => { server.closeAllConnections?.(); server.close(() => r()); }) });
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
    limit: num(a.limit ?? e.RPM_PROXY_LIMIT, 8),
    windowSecs: num(a.window ?? e.RPM_PROXY_WINDOW, 60),
    padMs: num(a.pad ?? e.RPM_PROXY_PAD, 250),
    port: num(a.port ?? e.RPM_PROXY_PORT, 8787),
    host: a.host ?? e.RPM_PROXY_HOST ?? "127.0.0.1",
    maxRetries: num(a["max-retries"] ?? e.RPM_PROXY_RETRIES, 5),
    retryBaseMs: num(a["retry-base"] ?? e.RPM_PROXY_RETRY_BASE, 5000),
    staggerMs: num(a.stagger ?? e.RPM_PROXY_STAGGER, 3000),
    limit429DelayMs: num(a["limit429-delay"] ?? e.RPM_PROXY_429_DELAY, 10000),
    timeoutDelayMs: num(a["timeout-delay"] ?? e.RPM_PROXY_TIMEOUT_DELAY, 30000),
    quiet: !!a.quiet,
    color: a.color === "false" ? false : a.color ?? (e.RPM_PROXY_COLOR === "false" ? false : e.RPM_PROXY_COLOR === "true" ? true : undefined),
  });
  const stop = async () => {
    await proxy.close();
    process.exit(0);
  };
  process.on("SIGINT", stop);
  process.on("SIGTERM", stop);
}
