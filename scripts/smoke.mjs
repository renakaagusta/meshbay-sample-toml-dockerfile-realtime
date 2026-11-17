#!/usr/bin/env node
// Dependency-free smoke test for meshbay-sample-realtime.
// Requires Node 22 or newer: it uses the global WebSocket client and streaming fetch.
//
// usage: node scripts/smoke.mjs https://<service>.renakaagusta.dev
//
// What it proves, in order:
//   1. GET /health answers {"status":"ok"} and names the replica that served it
//   2. GET / returns the chat page
//   3. GET /replicas lists at least one replica (all replicas when Redis is connected)
//   4. two WebSocket clients connect to /ws, a chat sent on one arrives on the other
//   5. GET /events streams 5 tick events, one per second, each carrying a replica id

const rawBaseURL = (process.argv[2] ?? "").trim();
if (rawBaseURL === "") {
  console.error("usage: node scripts/smoke.mjs https://<service>.renakaagusta.dev");
  process.exit(2);
}
if (typeof WebSocket === "undefined" || typeof fetch === "undefined") {
  console.error("FAIL this script needs Node 22 or newer (global WebSocket and fetch)");
  process.exit(2);
}

const baseURL = rawBaseURL.replace(/\/+$/, "");
const socketBaseURL = baseURL.replace(/^http/i, "ws");

function report(label, detail) {
  console.log("ok   " + label + (detail === undefined ? "" : " (" + detail + ")"));
}

function summary(messages) {
  return JSON.stringify(messages.slice(-4).map((message) => message.type));
}

async function fetchJSON(path) {
  const response = await fetch(baseURL + path, {
    headers: { accept: "application/json", "cache-control": "no-cache" },
    signal: AbortSignal.timeout(15_000),
  });
  const body = await response.json().catch(() => null);
  return { body, response };
}

function openSocket(label) {
  const socket = new WebSocket(socketBaseURL + "/ws");
  const received = [];
  const state = { closed: null };
  socket.addEventListener("message", (event) => {
    try {
      received.push(JSON.parse(String(event.data)));
    } catch {
      // A non-JSON frame is not something this service sends; ignore it instead of failing the run.
    }
  });
  socket.addEventListener("close", (event) => {
    state.closed = { code: event.code, reason: event.reason };
  });
  const opened = new Promise((resolve, reject) => {
    socket.addEventListener("open", () => resolve());
    socket.addEventListener("error", () => reject(new Error(label + ": the WebSocket connection failed")));
  });
  async function waitFor(matches, timeoutMilliseconds, description) {
    const deadline = Date.now() + timeoutMilliseconds;
    while (Date.now() < deadline) {
      const found = received.find(matches);
      if (found !== undefined) {
        return found;
      }
      if (state.closed !== null) {
        throw new Error(label + ": socket closed (code " + state.closed.code + ") while waiting for " + description);
      }
      await new Promise((resolve) => setTimeout(resolve, 50));
    }
    throw new Error(label + ": timeout waiting for " + description + ", frames seen " + summary(received));
  }
  return { opened, received, socket, state, waitFor };
}

function parseEventBlock(block) {
  const parsed = { data: "", event: "message", id: "" };
  for (const rawLine of block.split("\n")) {
    const line = rawLine.replace(/\r$/, "");
    if (line.startsWith("event:")) {
      parsed.event = line.slice("event:".length).trim();
    } else if (line.startsWith("data:")) {
      const chunk = line.slice("data:".length).trim();
      parsed.data = parsed.data === "" ? chunk : parsed.data + "\n" + chunk;
    } else if (line.startsWith("id:")) {
      parsed.id = line.slice("id:".length).trim();
    }
  }
  return parsed;
}

function withTimeout(promise, milliseconds, message) {
  return Promise.race([
    promise,
    new Promise((_, reject) => {
      setTimeout(() => reject(new Error(message)), milliseconds);
    }),
  ]);
}

async function checkHealth() {
  const health = await fetchJSON("/health");
  if (health.response.status !== 200) {
    throw new Error("GET /health returned HTTP " + health.response.status);
  }
  if (health.body === null || health.body.status !== "ok") {
    throw new Error("GET /health does not answer {\"status\":\"ok\"}");
  }
  if (typeof health.body.replica !== "string" || health.body.replica === "") {
    throw new Error("GET /health does not name a replica");
  }
  report("GET /health", "HTTP 200, version " + health.body.version + ", replica " + health.body.replica + ", redis " + health.body.redis);
  return health.body;
}

async function checkPage() {
  const response = await fetch(baseURL + "/", {
    headers: { accept: "text/html" },
    signal: AbortSignal.timeout(15_000),
  });
  if (response.status !== 200) {
    throw new Error("GET / returned HTTP " + response.status);
  }
  const contentType = response.headers.get("content-type") ?? "";
  if (!contentType.includes("text/html")) {
    throw new Error("GET / has content-type " + contentType + ", expected text/html");
  }
  const body = await response.text();
  if (!body.includes("meshbay-sample-realtime")) {
    throw new Error("GET / does not look like the chat page");
  }
  report("GET /", "HTTP 200, " + body.length + " bytes of HTML");
}

async function checkReplicas() {
  const replicas = await fetchJSON("/replicas");
  if (replicas.response.status !== 200) {
    throw new Error("GET /replicas returned HTTP " + replicas.response.status);
  }
  const records = Array.isArray(replicas.body?.replicas) ? replicas.body.replicas : [];
  if (records.length === 0) {
    throw new Error("GET /replicas returned no replica");
  }
  report(
    "GET /replicas",
    records.length + " replica(s): " + records.map((record) => record.id).join(", ") + (replicas.body.redis ? "" : " (redis not connected)"),
  );
  return records;
}

async function checkWebSockets() {
  const sender = openSocket("sender");
  const receiver = openSocket("receiver");
  try {
    await Promise.all([withTimeout(sender.opened, 15_000, "sender: timeout opening the socket"), withTimeout(receiver.opened, 15_000, "receiver: timeout opening the socket")]);
    const senderHello = await sender.waitFor((message) => message.type === "hello", 15_000, "the hello frame on the sender");
    const receiverHello = await receiver.waitFor((message) => message.type === "hello", 15_000, "the hello frame on the receiver");
    report("GET /ws opened twice", "sender replica " + senderHello.replica + ", receiver replica " + receiverHello.replica);

    const chatText = "smoke-" + Date.now();
    sender.socket.send(JSON.stringify({ text: chatText, type: "chat" }));
    await sender.waitFor((message) => message.type === "chat" && message.text === chatText, 15_000, "the echo of its own chat on the sender");
    const delivered = await receiver.waitFor((message) => message.type === "chat" && message.text === chatText, 15_000, "the chat on the receiver");

    const sameReplica = senderHello.replica === receiverHello.replica;
    const passedThroughRedis = delivered.origin !== receiverHello.replica;
    report(
      "chat sent on one socket arrived on the other",
      "origin " + delivered.origin + ", receiver " + receiverHello.replica + (sameReplica ? " (both sockets on one replica)" : passedThroughRedis ? " (cross-replica fan-out through Redis)" : " (two replicas, same origin)"),
    );
    return { receiverHello, sameReplica };
  } finally {
    sender.socket.close(1000, "smoke done");
    receiver.socket.close(1000, "smoke done");
  }
}

async function checkEvents() {
  const controller = new AbortController();
  const response = await fetch(baseURL + "/events", {
    headers: { accept: "text/event-stream", "accept-encoding": "identity", "cache-control": "no-cache" },
    signal: controller.signal,
  });
  if (response.status !== 200) {
    controller.abort();
    throw new Error("GET /events returned HTTP " + response.status);
  }
  const contentType = response.headers.get("content-type") ?? "";
  if (!contentType.includes("text/event-stream")) {
    controller.abort();
    throw new Error("GET /events has content-type " + contentType + ", expected text/event-stream");
  }

  const reader = response.body.getReader();
  const decoder = new TextDecoder();
  const ticks = [];
  let hello = null;
  let buffer = "";
  const startedAt = Date.now();
  try {
    while (ticks.length < 5 && Date.now() - startedAt < 25_000) {
      const chunk = await withTimeout(reader.read(), 10_000, "no SSE data for 10 s");
      if (chunk.done) {
        throw new Error("the SSE stream ended after " + ticks.length + " tick(s)");
      }
      buffer += decoder.decode(chunk.value, { stream: true });
      let boundary = buffer.indexOf("\n\n");
      while (boundary !== -1) {
        const parsed = parseEventBlock(buffer.slice(0, boundary));
        buffer = buffer.slice(boundary + 2);
        if (parsed.event === "hello" && parsed.data !== "") {
          hello = JSON.parse(parsed.data);
        }
        if (parsed.event === "tick" && parsed.data !== "") {
          ticks.push(JSON.parse(parsed.data));
        }
        boundary = buffer.indexOf("\n\n");
      }
    }
  } finally {
    controller.abort();
  }

  if (ticks.length < 5) {
    throw new Error("GET /events delivered " + ticks.length + " tick event(s), expected 5");
  }
  if (hello === null) {
    throw new Error("GET /events did not send the hello event");
  }
  for (const tick of ticks) {
    if (typeof tick.replica !== "string" || tick.replica === "") {
      throw new Error("a tick event carries no replica id: " + JSON.stringify(tick));
    }
    if (!Number.isInteger(tick.sequence)) {
      throw new Error("a tick event carries no integer sequence: " + JSON.stringify(tick));
    }
  }
  if (ticks.some((tick) => tick.replica !== hello.replica)) {
    throw new Error("the ticks come from another replica than the hello event");
  }
  for (let index = 1; index < ticks.length; index += 1) {
    const gap = Date.parse(ticks[index].at) - Date.parse(ticks[index - 1].at);
    if (!(gap > 300 && gap < 2_500)) {
      throw new Error("tick " + index + " arrived " + gap + " ms after the previous one, expected about 1000 ms");
    }
  }
  report("GET /events", "5 ticks in " + (Date.now() - startedAt) + " ms from replica " + ticks[0].replica + ", last sequence " + ticks[ticks.length - 1].sequence);
}

async function main() {
  const health = await checkHealth();
  await checkPage();
  const replicas = await checkReplicas();
  const sockets = await checkWebSockets();
  await checkEvents();

  const knownReplicas = new Set(replicas.map((record) => record.id));
  if (!knownReplicas.has(health.replica)) {
    throw new Error("/replicas does not list the replica named by /health");
  }
  if (!knownReplicas.has(sockets.receiverHello.replica)) {
    throw new Error("/replicas does not list the replica that served the WebSocket");
  }
}

main()
  .then(() => {
    console.log("OK");
    process.exit(0);
  })
  .catch((error) => {
    console.error("FAIL " + (error instanceof Error ? error.message : String(error)));
    process.exit(1);
  });
