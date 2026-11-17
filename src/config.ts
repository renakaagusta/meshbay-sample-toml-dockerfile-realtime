import { hostname } from "node:os";

export const applicationVersion = "v1";

export const startedAt = new Date();

function readInteger(name: string, fallback: number, minimum: number, maximum: number): number {
  const raw = (process.env[name] ?? "").trim();
  if (raw === "") {
    return fallback;
  }
  const value = Number.parseInt(raw, 10);
  if (!Number.isInteger(value) || value < minimum || value > maximum) {
    throw new Error(`${name} must be an integer between ${minimum} and ${maximum}, got "${raw}"`);
  }
  return value;
}

function readText(name: string, fallback: string): string {
  const raw = (process.env[name] ?? "").trim();
  return raw === "" ? fallback : raw;
}

export const config = {
  applicationVersion,
  channel: readText("CHANNEL", "meshbay-sample-realtime:chat"),
  heartbeatIntervalMs: readInteger("HEARTBEAT_INTERVAL_MS", 20_000, 1_000, 120_000),
  idleTimeoutSeconds: readInteger("IDLE_TIMEOUT_SECONDS", 240, 1, 255),
  port: readInteger("PORT", 8080, 1, 65_535),
  redisURL: (process.env.REDIS_URL ?? "").trim(),
  replicaID: readText("REPLICA_ID", hostname() || "replica-unknown"),
  retryMs: readInteger("SSE_RETRY_MS", 3_000, 100, 60_000),
  tickIntervalMs: readInteger("TICK_INTERVAL_MS", 1_000, 100, 60_000),
};

export function uptimeSeconds(): number {
  return Math.round((Date.now() - startedAt.getTime()) / 1_000);
}
