import { afterEach, beforeEach, describe, expect, it, vi } from "vitest";
import type { CursorStore } from "../src/CursorStore.js";
import { EventEngine } from "../src/EventEngine.js";
import { SorobanSubscriber } from "../src/SorobanSubscriber.js";

type StreamHandlers = {
  onmessage: (record: unknown) => void;
  onerror: (error: unknown) => void;
};

type MockStreamInstance = {
  cursor: string;
  handlers: StreamHandlers;
  close: ReturnType<typeof vi.fn>;
};

const streamInstances: MockStreamInstance[] = [];

vi.mock("@stellar/stellar-sdk", () => {
  class MockServer {
    operations() {
      return {
        cursor(cursor: string) {
          return {
            stream(handlers: StreamHandlers) {
              const close = vi.fn();
              streamInstances.push({ cursor, handlers, close });
              return close;
            },
          };
        },
      };
    }
  }
  return { Horizon: { Server: MockServer } };
});

function latestStream(): MockStreamInstance {
  const stream = streamInstances.at(-1);
  if (!stream) {
    throw new Error("Expected an active mock stream.");
  }
  return stream;
}

function makePaymentRecord(overrides: Record<string, unknown> = {}): Record<string, unknown> {
  return {
    type: "payment",
    id: "1",
    paging_token: "1",
    created_at: new Date().toISOString(),
    transaction_successful: true,
    source_account: "GABC",
    from: "GABC",
    to: "GDEF",
    amount: "10.0000000",
    asset_type: "native",
    ...overrides,
  };
}

class MemoryCursorStore implements CursorStore {
  private readonly cursors = new Map<string, string>();

  async get(streamKey: string): Promise<string | null> {
    return this.cursors.get(streamKey) ?? null;
  }

  async set(streamKey: string, cursor: string): Promise<void> {
    this.cursors.set(streamKey, cursor);
  }
}

class FakeSorobanRpc {
  public startCursors: Array<string | undefined> = [];
  public events: Array<{ id: string; pagingToken: string; topic: string[]; value: unknown }> = [];

  async getEvents(startCursor: string | undefined, limit: number): Promise<{ events: Array<{ id: string; pagingToken: string; topic: string[]; value: unknown }> }> {
    this.startCursors.push(startCursor);
    return { events: this.events };
  }
}

describe("EventEngine cursor resume", () => {
  beforeEach(() => {
    streamInstances.length = 0;
  });

  it("resumes the Horizon stream from the last stored cursor", async () => {
    const cursorStore = new MemoryCursorStore();
    const engine = new EventEngine({ network: "testnet", cursorStore });

    engine.start();
    await Promise.resolve();

    expect(latestStream().cursor).toBe("now");

    const watcher = engine.subscribe("GABC");
    const events: unknown[] = [];
    watcher.on("payment.sent", (evt) => events.push(evt));

    latestStream().handlers.onmessage(makePaymentRecord({ paging_token: "10", amount: "10.0000000" }));
    expect(events).toHaveLength(1);
    expect(await cursorStore.get("horizon:testnet")).toBe("10");

    engine.stop();

    const restartedEngine = new EventEngine({ network: "testnet", cursorStore });
    restartedEngine.start();
    await Promise.resolve();

    expect(latestStream().cursor).toBe("horizon:testnet");

    const restartedWatcher = restartedEngine.subscribe("GABC");
    const restartedEvents: unknown[] = [];
    restartedWatcher.on("payment.sent", (evt) => restartedEvents.push(evt));

    latestStream().handlers.onmessage(makePaymentRecord({ paging_token: "11", amount: "20.0000000" }));
    expect(restartedEvents).toHaveLength(1);
    expect(await cursorStore.get("horizon:testnet")).toBe("11");
  });

  it("continues Soroban stream from the stored cursor key", async () => {
    const cursorStore = new MemoryCursorStore();
    const fakeRpc = new FakeSorobanRpc();
    fakeRpc.events = [
      { id: "evt-1", pagingToken: "100", topic: [], value: "first" },
    ];

    const subscriber = new SorobanSubscriber({
      rpc: fakeRpc,
      cursorStore,
      streamKey: "soroban:testnet",
      onEvent: async () => {},
      pageSize: 10,
    });

    await subscriber.pollOnce();
    expect(fakeRpc.startCursors).toEqual([undefined]);
    expect(await cursorStore.get("soroban:testnet")).toBe("100");

    const nextRpc = new FakeSorobanRpc();
    nextRpc.events = [
      { id: "evt-2", pagingToken: "200", topic: [], value: "second" },
    ];
    const restartedSubscriber = new SorobanSubscriber({
      rpc: nextRpc,
      cursorStore,
      streamKey: "soroban:testnet",
      onEvent: async () => {},
      pageSize: 10,
    });

    await restartedSubscriber.pollOnce();
    expect(nextRpc.startCursors).toEqual(["100"]);
    expect(await cursorStore.get("soroban:testnet")).toBe("200");
  });

  it("delivers Horizon events even when cursor persistence fails", async () => {
    const warnCalls: Array<{ message: string; meta?: Record<string, unknown> }> = [];
    const logger = {
      info: () => {},
      warn: (message: string, meta?: Record<string, unknown>) => {
        warnCalls.push({ message, meta });
      },
      error: () => {},
    };

    const cursorStore: CursorStore = {
      async get() {
        return null;
      },
      async set() {
        throw new Error("cursor persist failed");
      },
    };

    const engine = new EventEngine({ network: "testnet", cursorStore, logger });

    engine.start();
    await Promise.resolve();

    const watcher = engine.subscribe("GABC");
    const received: unknown[] = [];
    watcher.on("payment.sent", (evt) => received.push(evt));

    latestStream().handlers.onmessage(makePaymentRecord({ paging_token: "50" }));

    expect(received).toHaveLength(1);
    expect(warnCalls.some((call) => call.message.includes("cursorStore.set() failed"))).toBe(true);
  });
});
