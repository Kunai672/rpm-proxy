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

test("defaults to a 3 second stagger and never overlaps upstream requests", async (t) => {
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
  });

  t.after(async () => {
    await proxy.close();
    await closeServer(upstream);
  });

  const statusResponse = await fetch(`http://127.0.0.1:${proxy.port}/__rpm/status`);
  const status = await statusResponse.json();
  assert.equal(status.staggerMs, 3000);
  assert.equal(status.maxConcurrentUpstream, 1);

  const responses = await Promise.all([
    post(proxy.port, "first"),
    post(proxy.port, "second"),
  ]);

  assert.deepEqual(responses.map(({ status }) => status), [200, 200]);
  assert.equal(maxActive, 1);
  assert.equal(starts.length, 2);
  assert.equal(ends.length, 2);
  assert.ok(starts[1] - ends[0] >= 2900, "the second request should wait the default 3 second stagger");
});

test("keeps retries serialized with other pooled requests", async (t) => {
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
      res.writeHead(429, { "content-type": "application/json" });
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
    staggerMs: 20,
    retryBaseMs: 20,
    maxRetries: 2,
  });

  t.after(async () => {
    await proxy.close();
    await closeServer(upstream);
  });

  const responses = await Promise.all([
    post(proxy.port, "flaky"),
    post(proxy.port, "steady-1"),
    post(proxy.port, "steady-2"),
    post(proxy.port, "steady-3"),
    post(proxy.port, "steady-4"),
  ]);

  assert.deepEqual(responses.map(({ status }) => status), [200, 200, 200, 200, 200]);
  assert.equal(flakyAttempts, 2);
  assert.equal(maxActive, 1);
});
