#!/usr/bin/env node
// Keeps one WebSocket and one SSE stream open for a while, logging what happens. Two uses:
//
//   long connection  node scripts/soak.mjs https://<service>.renakaagusta.dev 300
//                    exit 0 = both connections held for the whole duration (proves Caddy + Cloudflare keep them open)
//
//   redeploy         start it, redeploy the service while it runs.
//                    It reports the close and exits 1: that is the expected result, a redeploy ends the old
//                    containers. Open the page (or run the smoke test) afterwards to see the new replica.
//
// usage: node scripts/soak.mjs https://<service>.renakaagusta.dev [durationSeconds=150] [chatIntervalSeconds=10]

const rawBaseURL = (process.argv[2] ?? "").trim();
const durationSeconds = Number.parseInt(process.argv[3] ?? "150", 10);
const chatIntervalSeconds = Number.parseInt(process.argv[4] ?? "10", 10);

if (rawBaseURL === "" || !Number.isInteger(durationSeconds) || durationSeconds < 5 || !Number.isInteger(chatIntervalSeconds) || chatIntervalSeconds < 1) {
  console.error("usage: node scripts/soak.mjs https://<service>.renakaagusta.dev [durationSeconds=150] [chatIntervalSeconds=10]");
  process.exit(2);
}
if (typeof WebSocket === "undefined" || typeof fetch === "undefined") {
  console.error("FAIL this script needs Node 22 or newer (global WebSocket and fetch)");
  process.exit(2);
}

const baseURL = rawBaseURL.replace(/\/+$/, "");
const socketBaseURL = baseURL.replace(/^http/i, "ws");
const startedAt = Date.now();

function elapsed() {
  return ((Date.now() - startedAt) / 1000).toFixed(1) + "s";
}

function log(message) {
  console.log("[" + elapsed() + "] " + message);
}

const socket = new WebSocket(socketBaseURL + "/ws");
const counters = { chats: 0, heartbeats: 0, ticks: 0, ownEchoes: 0 };
let socketClosed = null;
let sseClosed = null;
let lastTickAt = Date.now();
let maximumTickGapMs = 0;
let sentChats = 0;
let replica = "";
let finishing = false;

function sendChat() {
  if (socket.readyState !== WebSocket.OPEN) {
    return;
  }
  sentChats += 1;
  socket.send(JSON.stringify({ text: "soak-" + sentChats + "-" + Date.now(), type: "chat" }));
}

socket.addEventListener("message", (event) => {
  let message;
  try {
    message = JSON.parse(String(event.data));
  } catch {
    return;
  }
  if (message.type === "hello") {
    replica = message.replica;
    log("ws hello: replica " + message.replica + ", client " + message.client + ", redis " + message.redis);
    sendChat();
  } else if (message.type === "chat") {
    counters.chats += 1;
    if (message.origin === replica) {
      counters.ownEchoes += 1;
    }
    log("ws chat from " + message.origin + ": " + message.text);
  } else if (message.type === "heartbeat") {
    counters.heartbeats += 1;
  } else if (message.type === "error") {
    log("ws error frame: " + message.error);
  }
});
socket.addEventListener("close", (event) => {
  if (finishing) {
    return;
  }
  socketClosed = { at: elapsed(), code: event.code, reason: event.reason };
  log("ws closed at " + socketClosed.at + ": code " + event.code + (event.reason === "" ? "" : ", reason " + event.reason));
});
socket.addEventListener("error", () => {
  log("ws error event (the close event carries the code)");
});

const chatTimer = setInterval(sendChat, chatIntervalSeconds * 1000);

const sseController = new AbortController();

async function readEvents() {
  const response = await fetch(baseURL + "/events", {
    headers: { accept: "text/event-stream", "accept-encoding": "identity", "cache-control": "no-cache" },
    signal: sseController.signal,
  });
  if (response.status !== 200) {
    sseClosed = { at: elapsed(), reason: "HTTP " + response.status };
    return;
  }
  const reader = response.body.getReader();
  const decoder = new TextDecoder();
  let buffer = "";
  while (true) {
    const chunk = await reader.read();
    if (chunk.done) {
      sseClosed = { at: elapsed(), reason: "stream ended" };
      log("sse stream ended at " + elapsed());
      return;
    }
    buffer += decoder.decode(chunk.value, { stream: true });
    let boundary = buffer.indexOf("\n\n");
    while (boundary !== -1) {
      const block = buffer.slice(0, boundary);
      buffer = buffer.slice(boundary + 2);
      for (const rawLine of block.split("\n")) {
        const line = rawLine.replace(/\r$/, "");
        if (!line.startsWith("data:")) {
          continue;
        }
        let payload;
        try {
          payload = JSON.parse(line.slice("data:".length).trim());
        } catch {
          continue;
        }
        if (typeof payload.sequence === "number") {
          counters.ticks += 1;
          const gap = Date.now() - lastTickAt;
          maximumTickGapMs = Math.max(maximumTickGapMs, gap);
          lastTickAt = Date.now();
          if (counters.ticks % 30 === 0) {
            log("sse tick " + counters.ticks + " from replica " + payload.replica + ", max gap " + maximumTickGapMs + " ms");
          }
        }
      }
      boundary = buffer.indexOf("\n\n");
    }
  }
}

readEvents().catch((error) => {
  if (sseController.signal.aborted) {
    return;
  }
  sseClosed = { at: elapsed(), reason: String(error) };
  log("sse read failed at " + elapsed() + ": " + String(error));
});

setTimeout(() => {
  finishing = true;
  clearInterval(chatTimer);
  sseController.abort();
  if (socket.readyState === WebSocket.OPEN) {
    socket.close(1000, "soak finished");
  }

  setTimeout(() => {
    console.log("");
    console.log("summary after " + elapsed() + ":");
    console.log("  replica       " + (replica === "" ? "unknown" : replica));
    console.log("  sse ticks     " + counters.ticks + " (max gap " + maximumTickGapMs + " ms)");
    console.log("  sse state     " + (sseClosed === null ? "open for the whole run" : sseClosed.reason + " at " + sseClosed.at));
    console.log("  ws chats      " + counters.chats + " received, " + counters.ownEchoes + " own, " + sentChats + " sent");
    console.log("  ws heartbeats " + counters.heartbeats);
    console.log("  ws state      " + (socketClosed === null ? "open for the whole run" : "closed at " + socketClosed.at + " with code " + socketClosed.code));

    if (socketClosed !== null || sseClosed !== null) {
      console.log("");
      console.log("not held: a connection ended before " + durationSeconds + " s. During a redeploy that is expected.");
      process.exit(1);
    }
    if (counters.ticks < 5) {
      console.log("");
      console.log("not held: the SSE stream delivered " + counters.ticks + " tick(s)");
      process.exit(1);
    }
    console.log("");
    console.log("OK held " + durationSeconds + " s");
    process.exit(0);
  }, 1_000);
}, durationSeconds * 1000);
