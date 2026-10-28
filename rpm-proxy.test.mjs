import assert from "node:assert/strict";
import { once } from "node:events";
import http from "node:http";
import test from "node:test";

import { startProxy } from "./rpm-proxy.mjs";

const delay = (ms) => new Promise((resolve) => setTimeout(resolve, ms));

async function listen(server) {
  server.listen(0, "127.0.0.1");
  await once(server, "listening");
  return server.address().port;
}

async function closeServer(server) {
  server.closeAllConnections?.();
  await new Promise((resolve, reject) => {
    server.close((error) => (error ? reject(error) : resolve()));
  });
}

async function post(port, requestId) {
  const response = await fetch(`http://127.0.0.1:${port}/v1/chat/completions`, {
    method: "POST",
    headers: {
      "content-type": "application/json",
      "x-request-id": requestId,
    },
    body: JSON.stringify({ model: "test-model", requestId }),
  });
  return { status: response.status, body: await response.text() };
}

test("concurrent requests when under capacity (default limit 8)", async (t) => {
  const starts = [];
  const ends = [];
  let active = 0;
  let maxActive = 0;

  const upstream = http.createServer((req, res) => {
    req.resume();
    active += 1;
    maxActive = Math.max(maxActive, active);
    starts.push(Date.now());

    setTimeout(() => {
      res.writeHead(200, { "content-type": "application/json" });
      res.end(JSON.stringify({ model: "test-model" }), () => {
        active -= 1;
        ends.push(Date.now());
      });
    }, 40);
  });

  const upstreamPort = await listen(upstream);
  const proxy = await startProxy({
    upstream: `http://127.0.0.1:${upstreamPort}`,
    host: "127.0.0.1",
    port: 0,
    quiet: true,
    limit: 8, // High limit
    staggerMs: 10,
  });

  t.after(async () => {
    await proxy.close();
    await closeServer(upstream);
  });

  const statusResponse = await fetch(`http://127.0.0.1:${proxy.port}/__rpm/status`);
  const status = await statusResponse.json();
  assert.equal(status.staggerMs, 10);

  // Fire 3 concurrent requests - now pooled and staggered
  const responses = await Promise.all([
    post(proxy.port, "first"),
    post(proxy.port, "second"),
    post(proxy.port, "third"),
  ]);

  assert.deepEqual(responses.map(({ status }) => status), [200, 200, 200]);
  assert.equal(maxActive, 1, "should serialize and stagger all requests");
});

test("serializes when at capacity (limit=1)", async (t) => {
  const starts = [];
  const ends = [];
  let active = 0;
  let maxActive = 0;

  const upstream = http.createServer((req, res) => {
    req.resume();
    active += 1;
    maxActive = Math.max(maxActive, active);
    starts.push(Date.now());

    setTimeout(() => {
      res.writeHead(200, { "content-type": "application/json" });
      res.end(JSON.stringify({ model: "test-model" }), () => {
        active -= 1;
        ends.push(Date.now());
      });
    }, 40);
  });

  const upstreamPort = await listen(upstream);
  const proxy = await startProxy({
    upstream: `http://127.0.0.1:${upstreamPort}`,
    host: "127.0.0.1",
    port: 0,
    quiet: true,
    limit: 1, // Low limit
    windowSecs: 1,
    staggerMs: 20,
  });

  t.after(async () => {
    await proxy.close();
    await closeServer(upstream);
  });

  const statusResponse = await fetch(`http://127.0.0.1:${proxy.port}/__rpm/status`);
  const status = await statusResponse.json();
  assert.equal(status.serializing, true);
  assert.equal(status.maxConcurrentUpstream, 1);

  const responses = await Promise.all([
    post(proxy.port, "first"),
    post(proxy.port, "second"),
    post(proxy.port, "third"),
  ]);

  assert.deepEqual(responses.map(({ status }) => status), [200, 200, 200]);
  assert.equal(maxActive, 1, "should serialize");
  assert.equal(starts.length, 3);
  assert.equal(ends.length, 3);
  assert.ok(starts[1] - ends[0] >= 15, "second request should wait stagger after first completes");
  assert.ok(starts[2] - ends[1] >= 15, "third request should wait stagger after second completes");
});

test("cooldown serializes new requests, not in-flight ones", async (t) => {
  let active = 0;
  let maxActive = 0;
  let flakyAttempts = 0;

  const upstream = http.createServer((req, res) => {
    req.resume();
    active += 1;
    maxActive = Math.max(maxActive, active);

    const finish = () => {
      active -= 1;
    };

    if (req.headers["x-request-id"] === "flaky" && flakyAttempts++ === 0) {
      res.writeHead(429, { "content-type": "application/json", "retry-after": "0" });
      res.end(JSON.stringify({ error: "rate limited" }), finish);
      return;
    }

    setTimeout(() => {
      res.writeHead(200, { "content-type": "application/json" });
      res.end(JSON.stringify({ model: "test-model" }), finish);
    }, 40);
  });

  const upstreamPort = await listen(upstream);
  const proxy = await startProxy({
    upstream: `http://127.0.0.1:${upstreamPort}`,
    host: "127.0.0.1",
    port: 0,
    quiet: true,
    limit: 8,
    staggerMs: 20,
    retryBaseMs: 20,
    maxRetries: 2,
    limit429DelayMs: 50,
  });

  t.after(async () => {
    await proxy.close();
    await closeServer(upstream);
  });

  // All requests are pooled and staggered now
  const responses = await Promise.all([
    post(proxy.port, "flaky"),
    post(proxy.port, "steady-1"),
    post(proxy.port, "steady-2"),
    post(proxy.port, "steady-3"),
    post(proxy.port, "steady-4"),
  ]);

  assert.deepEqual(responses.map(({ status }) => status), [200, 200, 200, 200, 200]);
  assert.equal(flakyAttempts, 2, "flaky request should retry once");
  assert.equal(maxActive, 1, "all requests serialized with stagger");
});

test("allows concurrency after cooldown expires", async (t) => {
  let active = 0;
  let maxActive = 0;
  let flakyAttempts = 0;

  const upstream = http.createServer((req, res) => {
    req.resume();
    active += 1;
    maxActive = Math.max(maxActive, active);

    const finish = () => {
      active -= 1;
    };

    if (req.headers["x-request-id"] === "flaky" && flakyAttempts++ === 0) {
      res.writeHead(429, { "content-type": "application/json", "retry-after": "0" });
      res.end(JSON.stringify({ error: "rate limited" }), finish);
      return;
    }

    setTimeout(() => {
      res.writeHead(200, { "content-type": "application/json" });
      res.end(JSON.stringify({ model: "test-model" }), finish);
    }, 40);
  });

  const upstreamPort = await listen(upstream);
  const proxy = await startProxy({
    upstream: `http://127.0.0.1:${upstreamPort}`,
    host: "127.0.0.1",
    port: 0,
    quiet: true,
    limit: 8,
    staggerMs: 20,
    retryBaseMs: 20,
    maxRetries: 2,
    limit429DelayMs: 50,
  });

  t.after(async () => {
    await proxy.close();
    await closeServer(upstream);
  });

  await post(proxy.port, "flaky");
  await delay(200);

  const responses = await Promise.all([
    post(proxy.port, "steady-1"),
    post(proxy.port, "steady-2"),
    post(proxy.port, "steady-3"),
  ]);

  assert.deepEqual(responses.map(({ status }) => status), [200, 200, 200]);
  assert.equal(maxActive, 1, "still serialized with stagger");
});