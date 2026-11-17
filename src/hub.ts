import { config } from "./config";

export type ChatMessage = {
  at: string;
  id: string;
  origin: string;
  text: string;
  type: "chat";
};

export type HeartbeatMessage = {
  at: string;
  clients: number;
  origin: string;
  type: "heartbeat";
};

export type ServerMessage = ChatMessage | HeartbeatMessage;

export type ClientSender = (payload: string) => void;

/**
 * Keeps the WebSocket senders of this replica and forwards a message either through Redis pub/sub (so every replica
 * delivers it once) or, without Redis, straight to the local senders.
 */
export class RealtimeHub {
  readonly #senders = new Map<string, ClientSender>();

  #publish: ((payload: string) => boolean) | null = null;

  #clientCounter = 0;

  get clientCount(): number {
    return this.#senders.size;
  }

  addClient(sender: ClientSender): string {
    this.#clientCounter += 1;
    const clientID = `${config.replicaID}-${this.#clientCounter}`;
    this.#senders.set(clientID, sender);
    return clientID;
  }

  removeClient(clientID: string): void {
    this.#senders.delete(clientID);
  }

  setPublish(publish: ((payload: string) => boolean) | null): void {
    this.#publish = publish;
  }

  broadcast(message: ServerMessage): void {
    const payload = JSON.stringify(message);
    if (this.#publish !== null && this.#publish(payload)) {
      // The pub/sub subscription echoes the payload back to every replica, this one included.
      return;
    }
    this.broadcastLocal(payload);
  }

  broadcastLocal(payload: string): void {
    for (const [clientID, sender] of this.#senders) {
      try {
        sender(payload);
      } catch {
        this.#senders.delete(clientID);
      }
    }
  }
}

export const realtimeHub = new RealtimeHub();
