import type { NormalizedEvent } from "@orbital/pulse-core";

type ConnectionKey = {
  serverUrl: string;
  address: string;
  token?: string;
};

type ConnectionSubscriber = {
  onOpen: () => void;
  onEvent: (event: NormalizedEvent) => void;
  onParseError: () => void;
  onError: () => void;
};

type ConnectionEntry = {
  source: EventSource;
  subscribers: Set<ConnectionSubscriber>;
  connected: boolean;
  serverUrl: string;
  address: string;
  lastEventAt: number | null;
};

const pool = new Map<string, ConnectionEntry>();
const devtoolsObservers = new Set<() => void>();

function getConnectionKey({ serverUrl, address, token }: ConnectionKey): string {
  return JSON.stringify([serverUrl, address, token ?? ""]);
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

function notifyDevtools() {
  for (const observer of devtoolsObservers) {
    observer();
  }
}

export function acquireEventConnection(
  key: ConnectionKey,
  subscriber: ConnectionSubscriber
) {
  const poolKey = getConnectionKey(key);
  let entry = pool.get(poolKey);

  if (!entry) {
    const newEntry: ConnectionEntry = {
      source: new EventSource(getEventSourceUrl(key)),
      subscribers: new Set(),
      connected: false,
      serverUrl: key.serverUrl,
      address: key.address,
      lastEventAt: null,
    };

    newEntry.source.onopen = () => {
      newEntry.connected = true;
      notifySubscribers(newEntry, (current) => current.onOpen());
      notifyDevtools();
    };

    newEntry.source.onmessage = (message) => {
      try {
        const event = JSON.parse(message.data) as NormalizedEvent;
        newEntry.lastEventAt = Date.now();
        notifySubscribers(newEntry, (current) => current.onEvent(event));
        notifyDevtools();
      } catch {
        notifySubscribers(newEntry, (current) => current.onParseError());
      }
    };

    newEntry.source.onerror = () => {
      newEntry.connected = false;
      notifySubscribers(newEntry, (current) => current.onError());
      notifyDevtools();
    };

    pool.set(poolKey, newEntry);
    entry = newEntry;
  }

  entry.subscribers.add(subscriber);

  return {
    connected: entry.connected,
    unsubscribe: () => {
      entry.subscribers.delete(subscriber);

      if (entry.subscribers.size === 0) {
        entry.source.close();
        pool.delete(poolKey);
        notifyDevtools();
      }
    },
  };
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

/**
 * DevTools API: Get a snapshot of all active connections.
 * @internal
 */
export function __devtoolsGetConnections() {
  return Array.from(pool.entries()).map(([key, entry]) => ({
    key,
    serverUrl: entry.serverUrl,
    address: entry.address,
    connected: entry.connected,
    subscriberCount: entry.subscribers.size,
    lastEventAt: entry.lastEventAt,
  }));
}

/**
 * DevTools API: Subscribe to pool changes for real-time updates.
 * Returns an unsubscribe function.
 * @internal
 */
export function __devtoolsSubscribe(callback: () => void): () => void {
  devtoolsObservers.add(callback);
  return () => {
    devtoolsObservers.delete(callback);
  };
}
