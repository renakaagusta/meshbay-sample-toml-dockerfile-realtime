import { createClient } from "redis";

import { applicationVersion, config, startedAt } from "./config";
import { realtimeHub } from "./hub";

const replicaKeyPrefix = "meshbay-sample-realtime:replicas:";
const registrationIntervalMs = 10_000;
const registrationTTLSeconds = 30;

export type ReplicaRecord = {
  id: string;
  registered_at: string;
  started_at: string;
  version: string;
};

type RedisClient = ReturnType<typeof createClient>;

let publisher: RedisClient | null = null;
let subscriber: RedisClient | null = null;
let registrationTimer: ReturnType<typeof setInterval> | null = null;

function log(message: string): void {
  console.log(`[fanout] ${message}`);
}

export function redisConnected(): boolean {
  return publisher !== null && publisher.isReady;
}

function selfRecord(): ReplicaRecord {
  return {
    id: config.replicaID,
    registered_at: new Date().toISOString(),
    started_at: startedAt.toISOString(),
    version: applicationVersion,
  };
}

async function closeClient(client: RedisClient | null): Promise<void> {
  if (client === null || !client.isOpen) {
    return;
  }
  try {
    await client.quit();
  } catch (error) {
    log(`could not close a redis client: ${String(error)}`);
  }
}

async function registerReplica(client: RedisClient): Promise<void> {
  try {
    await client.set(`${replicaKeyPrefix}${config.replicaID}`, JSON.stringify(selfRecord()), { EX: registrationTTLSeconds });
  } catch (error) {
    log(`could not register this replica: ${String(error)}`);
  }
}

/**
 * Connects lazily: the HTTP server answers immediately and the fan-out is switched on once Redis is reachable, so a
 * project without a Redis database (or with a typo in REDIS_URL) still serves the local-only variant.
 */
async function connectRedis(): Promise<void> {
  const socket = {
    connectTimeout: 5_000,
    reconnectStrategy: (retries: number) => (retries > 10 ? new Error("redis reconnect attempts exhausted") : Math.min(250 * retries, 3_000)),
  };
  let nextPublisher: RedisClient | null = null;
  let nextSubscriber: RedisClient | null = null;
  try {
    nextPublisher = createClient({ socket, url: config.redisURL });
    nextPublisher.on("error", (error: unknown) => log(`publisher error: ${String(error)}`));
    await nextPublisher.connect();

    nextSubscriber = nextPublisher.duplicate();
    nextSubscriber.on("error", (error: unknown) => log(`subscriber error: ${String(error)}`));
    await nextSubscriber.connect();
    await nextSubscriber.subscribe(config.channel, (rawPayload: string) => {
      realtimeHub.broadcastLocal(rawPayload);
    });

    publisher = nextPublisher;
    subscriber = nextSubscriber;
    realtimeHub.setPublish((payload: string) => {
      if (publisher === null || !publisher.isReady) {
        return false;
      }
      publisher.publish(config.channel, payload).catch((error: unknown) => log(`publish failed: ${String(error)}`));
      return true;
    });

    await registerReplica(publisher);
    registrationTimer = setInterval(() => {
      if (publisher !== null) {
        void registerReplica(publisher);
      }
    }, registrationIntervalMs);
    log(`pub/sub enabled on channel ${config.channel} as replica ${config.replicaID}`);
  } catch (error) {
    log(`redis unavailable (${String(error)}), using local broadcast only`);
    await closeClient(nextSubscriber);
    await closeClient(nextPublisher);
  }
}

export function startFanout(): void {
  if (config.redisURL === "") {
    log("REDIS_URL is not set, using local broadcast only (one replica)");
    return;
  }
  void connectRedis();
}

export async function describeReplicas(): Promise<ReplicaRecord[]> {
  const records = new Map<string, ReplicaRecord>();
  records.set(config.replicaID, selfRecord());
  if (publisher === null || !publisher.isReady) {
    return [...records.values()];
  }
  const client = publisher;
  try {
    const keys: string[] = [];
    let cursor = 0;
    do {
      const page = await client.scan(cursor, { COUNT: 100, MATCH: `${replicaKeyPrefix}*` });
      keys.push(...page.keys);
      cursor = page.cursor;
    } while (cursor !== 0);
    if (keys.length > 0) {
      const values = await client.mGet(keys);
      for (const value of values) {
        if (value === null) {
          continue;
        }
        const record = JSON.parse(value) as ReplicaRecord;
        records.set(record.id, record);
      }
    }
  } catch (error) {
    log(`could not list replicas: ${String(error)}`);
  }
  return [...records.values()].sort((left, right) => left.id.localeCompare(right.id));
}

export async function stopFanout(): Promise<void> {
  if (registrationTimer !== null) {
    clearInterval(registrationTimer);
    registrationTimer = null;
  }
  const openSubscriber = subscriber;
  const openPublisher = publisher;
  publisher = null;
  subscriber = null;
  realtimeHub.setPublish(null);
  await closeClient(openSubscriber);
  await closeClient(openPublisher);
}
