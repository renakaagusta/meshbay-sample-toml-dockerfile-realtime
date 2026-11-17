import type { ServerWebSocket } from "bun";
import { Hono } from "hono";
import { createBunWebSocket } from "hono/bun";
import { cors } from "hono/cors";
import { streamSSE } from "hono/streaming";

import { applicationVersion, config, startedAt, uptimeSeconds } from "./config";
import { describeReplicas, redisConnected, startFanout, stopFanout } from "./fanout";
import { realtimeHub, type ClientSender } from "./hub";
import { renderPage } from "./page";

const { upgradeWebSocket, websocket } = createBunWebSocket<ServerWebSocket>();

const application = new Hono();

application.use("*", cors());

application.get("/", (context) => context.html(renderPage()));

application.get("/health", (context) =>
  context.json({
    clients: realtimeHub.clientCount,
    redis: redisConnected(),
    replica: config.replicaID,
    started_at: startedAt.toISOString(),
    status: "ok",
    time: new Date().toISOString(),
    uptime_seconds: uptimeSeconds(),
    version: applicationVersion,
  }),
);

application.get("/replicas", async (context) =>
  context.json({
    redis: redisConnected(),
    replica: config.replicaID,
    replicas: await describeReplicas(),
  }),
);

application.get("/events", (context) => {
  // Cloudflare and Caddy must not cache or buffer the stream.
  context.header("Cache-Control", "no-cache, no-transform");
  context.header("X-Accel-Buffering", "no");
  return streamSSE(context, async (stream) => {
    let aborted = false;
    stream.onAbort(() => {
      aborted = true;
    });
    let sequence = 0;
    try {
      await stream.writeSSE({
        data: JSON.stringify({
          at: new Date().toISOString(),
          redis: redisConnected(),
          replica: config.replicaID,
          version: applicationVersion,
        }),
        event: "hello",
        retry: config.retryMs,
      });
      while (!aborted) {
        sequence += 1;
        await stream.writeSSE({
          data: JSON.stringify({
            at: new Date().toISOString(),
            clients: realtimeHub.clientCount,
            replica: config.replicaID,
            sequence,
          }),
          event: "tick",
          id: `${config.replicaID}-${sequence}`,
        });
        await stream.sleep(config.tickIntervalMs);
      }
    } catch (error) {
      if (!aborted) {
        console.error(`sse stream failed: ${String(error)}`);
      }
    }
  });
});

application.get(
  "/ws",
  upgradeWebSocket(() => {
    let clientID = "";
    return {
      onClose() {
        realtimeHub.removeClient(clientID);
      },
      onMessage(event, socket) {
        let payload: unknown;
        try {
          payload = JSON.parse(String(event.data));
        } catch {
          socket.send(JSON.stringify({ error: "message must be JSON", type: "error" }));
          return;
        }
        const message = (typeof payload === "object" && payload !== null ? payload : {}) as { text?: unknown; type?: unknown };
        if (message.type === "ping") {
          socket.send(JSON.stringify({ at: new Date().toISOString(), replica: config.replicaID, type: "pong" }));
          return;
        }
        const text = typeof message.text === "string" ? message.text.trim() : "";
        if (message.type !== "chat" || text === "") {
          socket.send(JSON.stringify({ error: 'expected {"type":"chat","text":"..."}', type: "error" }));
          return;
        }
        realtimeHub.broadcast({
          at: new Date().toISOString(),
          id: crypto.randomUUID(),
          origin: config.replicaID,
          text: text.slice(0, 800),
          type: "chat",
        });
      },
      onOpen(_event, socket) {
        const sender: ClientSender = (payload) => {
          socket.send(payload);
        };
        clientID = realtimeHub.addClient(sender);
        socket.send(
          JSON.stringify({
            at: new Date().toISOString(),
            client: clientID,
            clients: realtimeHub.clientCount,
            redis: redisConnected(),
            replica: config.replicaID,
            type: "hello",
            version: applicationVersion,
          }),
        );
      },
    };
  }),
);

setInterval(() => {
  realtimeHub.broadcastLocal(
    JSON.stringify({
      at: new Date().toISOString(),
      clients: realtimeHub.clientCount,
      origin: config.replicaID,
      type: "heartbeat",
    }),
  );
}, config.heartbeatIntervalMs);

startFanout();

const server = Bun.serve({
  fetch: application.fetch,
  idleTimeout: config.idleTimeoutSeconds,
  port: config.port,
  websocket,
});

console.log(
  `meshbay-sample-realtime ${applicationVersion} on :${server.port} replica=${config.replicaID} redis=${config.redisURL === "" ? "disabled" : "configured"}`,
);

async function shutdown(signal: string): Promise<void> {
  console.log(`received ${signal}, closing connections`);
  await server.stop(true);
  await stopFanout();
  process.exit(0);
}

process.on("SIGTERM", () => {
  void shutdown("SIGTERM");
});
process.on("SIGINT", () => {
  void shutdown("SIGINT");
});
