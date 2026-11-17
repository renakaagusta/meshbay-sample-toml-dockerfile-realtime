#!/usr/bin/env node
// Proves the Redis fan-out between two instances: a chat frame written on the first instance must arrive on a socket
// connected to the second one. Needs both instances up with the same REDIS_URL, which is exactly the two-replica case.
//
// usage: node scripts/fanout-check.mjs http://127.0.0.1:8081 http://127.0.0.1:8082

const [firstBaseURL, secondBaseURL] = process.argv.slice(2).map((argument) => argument.trim().replace(/\/+$/, ""));
if (!firstBaseURL || !secondBaseURL) {
  console.error("usage: node scripts/fanout-check.mjs http://127.0.0.1:8081 http://127.0.0.1:8082");
  process.exit(2);
}
if (typeof WebSocket === "undefined") {
  console.error("FAIL this script needs Node 22 or newer (global WebSocket)");
  process.exit(2);
}

function socketURL(baseURL) {
  return baseURL.replace(/^http/i, "ws") + "/ws";
}

function connect(label, baseURL) {
  const socket = new WebSocket(socketURL(baseURL));
  const received = [];
  socket.addEventListener("message", (event) => {
    try {
      received.push(JSON.parse(String(event.data)));
    } catch {
      // ignore non-JSON frames
    }
  });
  const opened = new Promise((resolve, reject) => {
    socket.addEventListener("open", () => resolve());
    socket.addEventListener("error", () => reject(new Error(label + ": could not connect to " + baseURL)));
  });
  return { opened, received, socket };
}

async function waitFor(client, matches, description) {
  const deadline = Date.now() + 15_000;
  while (Date.now() < deadline) {
    const found = client.received.find(matches);
    if (found !== undefined) {
      return found;
    }
    await new Promise((resolve) => setTimeout(resolve, 50));
  }
  throw new Error("timeout waiting for " + description);
}

async function waitForRedis(baseURL) {
  const deadline = Date.now() + 20_000;
  while (Date.now() < deadline) {
    const response = await fetch(baseURL + "/health").catch(() => null);
    const body = response === null ? null : await response.json().catch(() => null);
    if (body !== null && body.status === "ok" && body.redis === true) {
      return body;
    }
    await new Promise((resolve) => setTimeout(resolve, 500));
  }
  throw new Error(baseURL + ": /health did not report redis true within 20 s (check REDIS_URL)");
}

async function main() {
  const firstHealth = await waitForRedis(firstBaseURL);
  const secondHealth = await waitForRedis(secondBaseURL);
  if (firstHealth.replica === secondHealth.replica) {
    throw new Error("both instances report the same replica " + firstHealth.replica + ", set a different REPLICA_ID");
  }
  const first = connect("first", firstBaseURL);
  const second = connect("second", secondBaseURL);
  try {
    await Promise.all([first.opened, second.opened]);
    const firstHello = await waitFor(first, (message) => message.type === "hello", "the hello frame on the first instance");
    const secondHello = await waitFor(second, (message) => message.type === "hello", "the hello frame on the second instance");
    if (!firstHello.redis || !secondHello.redis) {
      throw new Error("one instance is not connected to Redis (redis false in hello)");
    }
    if (firstHello.replica === secondHello.replica) {
      throw new Error("both instances report the same replica " + firstHello.replica + ", set a different REPLICA_ID or a different hostname");
    }
    console.log("ok   hello  (first " + firstHello.replica + ", second " + secondHello.replica + ")");

    const chatText = "fanout-" + Date.now();
    first.socket.send(JSON.stringify({ text: chatText, type: "chat" }));
    const delivered = await waitFor(second, (message) => message.type === "chat" && message.text === chatText, "the chat on the second instance");
    if (delivered.origin !== firstHello.replica) {
      throw new Error("the chat on the second instance came from " + delivered.origin + ", expected " + firstHello.replica);
    }
    console.log("ok   chat from " + delivered.origin + " arrived on " + secondHello.replica);
  } finally {
    first.socket.close(1000, "fanout check done");
    second.socket.close(1000, "fanout check done");
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
