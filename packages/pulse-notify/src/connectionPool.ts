import type { NormalizedEvent } from "@orbital/pulse-core";

type ConnectionKey = {
  serverUrl: string;
  address: string;
  token?: string;
  withCredentials?: boolean;
};

type ConnectionSubscriber = {
  onOpen: () => void;
  onEvent: (event: NormalizedEvent) => void;
  onParseError: () => void;
  onError: () => void;
};

type ConnectionEntry = {
  key: ConnectionKey;
  url: string;
  source: EventSource;
  subscribers: Set<ConnectionSubscriber>;
  connected: boolean;
  lastEventAt: string | null;
};

const pool = new Map<string, ConnectionEntry>();

function getConnectionKey({ serverUrl, address, token, withCredentials }: ConnectionKey): string {
  return JSON.stringify([serverUrl, address, token ?? "", withCredentials ?? false]);
}

function getEventSourceUrl({ serverUrl, address, token }: ConnectionKey): string {
  const base = `${serverUrl}/events/${address}`;
  return token ? `${base}?token=${encodeURIComponent(token)}` : base;
}

function notifySubscribers(
  entry: ConnectionEntry,
  notify: (subscriber: ConnectionSubscriber) => void
) {
  for (const subscriber of [...entry.subscribers]) {
    notify(subscriber);
  }
}

export function acquireEventConnection(
  key: ConnectionKey,
  subscriber: ConnectionSubscriber
) {
  const poolKey = getConnectionKey(key);
  let entry = pool.get(poolKey);

  if (!entry) {
    const connectionUrl = getEventSourceUrl(key);
    const newEntry: ConnectionEntry = {
      key,
      url: connectionUrl,
      source: new EventSource(connectionUrl, key.withCredentials ? { withCredentials: true } : undefined),
      subscribers: new Set(),
      connected: false,
      lastEventAt: null,
    };

    newEntry.source.onopen = () => {
      newEntry.connected = true;
      notifySubscribers(newEntry, (current) => current.onOpen());
    };

    newEntry.source.onmessage = (message) => {
      try {
        const event = JSON.parse(message.data) as NormalizedEvent;
        newEntry.lastEventAt = new Date().toISOString();
        notifySubscribers(newEntry, (current) => current.onEvent(event));
      } catch {
        notifySubscribers(newEntry, (current) => current.onParseError());
      }
    };

    newEntry.source.onerror = () => {
      newEntry.connected = false;
      notifySubscribers(newEntry, (current) => current.onError());
    };

    pool.set(poolKey, newEntry);
    entry = newEntry;
  }

  entry.subscribers.add(subscriber);

  return {
    get connected() {
      return entry.connected;
    },
    unsubscribe: () => {
      entry.subscribers.delete(subscriber);

      if (entry.subscribers.size === 0) {
        entry.source.close();
        pool.delete(poolKey);
      }
    },
  };
}

export type ConnectionPoolDebugEntry = {
  serverUrl: string;
  address: string;
  token?: string;
  withCredentials: boolean;
  connected: boolean;
  subscriberCount: number;
  lastEventAt: string | null;
  url: string;
};

export function __getConnectionPoolSnapshot(): ConnectionPoolDebugEntry[] {
  return Array.from(pool.values()).map((entry) => ({
    serverUrl: entry.key.serverUrl,
    address: entry.key.address,
    token: entry.key.token,
    withCredentials: entry.key.withCredentials ?? false,
    connected: entry.connected,
    subscriberCount: entry.subscribers.size,
    lastEventAt: entry.lastEventAt,
    url: entry.url,
  }));
}

export function __getConnectionPoolSizeForTests() {
  return pool.size;
}

export function __resetConnectionPoolForTests() {
  for (const entry of pool.values()) {
    entry.source.close();
  }
  pool.clear();
}
