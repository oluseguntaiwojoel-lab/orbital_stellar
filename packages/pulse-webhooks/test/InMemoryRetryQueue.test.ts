import { describe, expect, it, beforeEach, afterEach, vi } from "vitest";

import { InMemoryRetryQueue } from "../src/adapters/InMemoryRetryQueue.js";
import type { RetryRecord } from "../src/RetryQueue.js";

const baseRecord: Omit<RetryRecord, "id" | "nextRetryAt" | "createdAt"> = {
  event: {
    type: "payment.received",
    to: "GDEST",
    from: "GSRC",
    amount: "10",
    asset: "XLM",
    timestamp: "2026-04-26T12:00:00.000Z",
    raw: { id: "evt_1" },
  },
  url: "https://example.com/webhook",
  attempt: 1,
};

describe("InMemoryRetryQueue", () => {
  let queue: InMemoryRetryQueue;

  beforeEach(() => {
    vi.useFakeTimers();
    vi.setSystemTime(new Date("2026-04-27T00:00:00.000Z"));
    queue = new InMemoryRetryQueue();
  });

  afterEach(() => {
    vi.useRealTimers();
  });

  it("enqueues and dequeues records in nextRetryAt order", async () => {
    const now = Date.now();

    const record1: RetryRecord = {
      ...baseRecord,
      id: "rec_1",
      nextRetryAt: now + 3000,
      createdAt: now,
    };

    const record2: RetryRecord = {
      ...baseRecord,
      id: "rec_2",
      nextRetryAt: now + 1000,
      createdAt: now,
    };

    const record3: RetryRecord = {
      ...baseRecord,
      id: "rec_3",
      nextRetryAt: now + 2000,
      createdAt: now,
    };

    // Enqueue out of order
    await queue.enqueue(record1);
    await queue.enqueue(record2);
    await queue.enqueue(record3);

    // First ready record should be record2 (earliest nextRetryAt)
    vi.advanceTimersByTime(1500);

    const dequeued1 = await queue.dequeue();
    expect(dequeued1?.id).toBe("rec_2");

    vi.advanceTimersByTime(1000); // Total 2500ms

    const dequeued2 = await queue.dequeue();
    expect(dequeued2?.id).toBe("rec_3");

    vi.advanceTimersByTime(1000); // Total 3500ms

    const dequeued3 = await queue.dequeue();
    expect(dequeued3?.id).toBe("rec_1");
  });

  it("returns null when no records are ready for dequeue", async () => {
    const now = Date.now();

    await queue.enqueue({
      ...baseRecord,
      id: "rec_1",
      nextRetryAt: now + 5000,
      createdAt: now,
    });

    // nextRetryAt hasn't arrived yet
    const result = await queue.dequeue();
    expect(result).toBeNull();
  });

  it("returns null when queue is empty", async () => {
    const result = await queue.dequeue();
    expect(result).toBeNull();
  });

  it("acknowledges and removes a record from the queue", async () => {
    const now = Date.now();

    const record: RetryRecord = {
      ...baseRecord,
      id: "rec_1",
      nextRetryAt: now,
      createdAt: now,
    };

    await queue.enqueue(record);
    expect(await queue.size()).toBe(1);

    await queue.ack("rec_1");
    expect(await queue.size()).toBe(0);

    const dequeued = await queue.dequeue();
    expect(dequeued).toBeNull();
  });

  it("clears all records from the queue", async () => {
    const now = Date.now();

    for (let i = 1; i <= 5; i++) {
      await queue.enqueue({
        ...baseRecord,
        id: `rec_${i}`,
        nextRetryAt: now + i * 1000,
        createdAt: now,
      });
    }

    expect(await queue.size()).toBe(5);

    await queue.clear();
    expect(await queue.size()).toBe(0);

    const dequeued = await queue.dequeue();
    expect(dequeued).toBeNull();
  });

  it("returns the correct queue size", async () => {
    const now = Date.now();

    expect(await queue.size()).toBe(0);

    await queue.enqueue({
      ...baseRecord,
      id: "rec_1",
      nextRetryAt: now + 1000,
      createdAt: now,
    });
    expect(await queue.size()).toBe(1);

    await queue.enqueue({
      ...baseRecord,
      id: "rec_2",
      nextRetryAt: now + 2000,
      createdAt: now,
    });
    expect(await queue.size()).toBe(2);

    vi.advanceTimersByTime(1500);

    await queue.dequeue(); // Remove rec_1
    expect(await queue.size()).toBe(1);

    await queue.clear();
    expect(await queue.size()).toBe(0);
  });

  it("handles re-enqueueing the same record ID (updates it)", async () => {
    const now = Date.now();

    const record1: RetryRecord = {
      ...baseRecord,
      id: "rec_1",
      nextRetryAt: now + 1000,
      createdAt: now,
    };

    const record1Updated: RetryRecord = {
      ...baseRecord,
      id: "rec_1",
      nextRetryAt: now + 3000,
      createdAt: now,
    };

    await queue.enqueue(record1);
    expect(await queue.size()).toBe(1);

    // Re-enqueue with same ID but different nextRetryAt
    await queue.enqueue(record1Updated);
    expect(await queue.size()).toBe(1); // Still only 1 record

    // First dequeue attempt should not return anything (both times still in future)
    let dequeued = await queue.dequeue();
    expect(dequeued).toBeNull();

    // Advance to time between old and new nextRetryAt (1500ms)
    vi.advanceTimersByTime(1500);

    // Should still not be ready (new nextRetryAt is 3000ms)
    dequeued = await queue.dequeue();
    expect(dequeued).toBeNull();

    // Advance to 3500ms
    vi.advanceTimersByTime(2000);

    // Now it should be ready
    dequeued = await queue.dequeue();
    expect(dequeued?.id).toBe("rec_1");
    expect(dequeued?.nextRetryAt).toBe(now + 3000);
  });

  it("does not dequeue a record when nextRetryAt equals now", async () => {
    const now = Date.now();

    await queue.enqueue({
      ...baseRecord,
      id: "rec_1",
      nextRetryAt: now,
      createdAt: now,
    });

    // At exactly now, should be ready (nextRetryAt <= now)
    const dequeued = await queue.dequeue();
    expect(dequeued?.id).toBe("rec_1");
  });

  it("handles multiple records with the same nextRetryAt", async () => {
    const now = Date.now();
    const sameTime = now + 2000;

    await queue.enqueue({
      ...baseRecord,
      id: "rec_1",
      nextRetryAt: sameTime,
      createdAt: now,
    });

    await queue.enqueue({
      ...baseRecord,
      id: "rec_2",
      nextRetryAt: sameTime,
      createdAt: now,
    });

    await queue.enqueue({
      ...baseRecord,
      id: "rec_3",
      nextRetryAt: sameTime,
      createdAt: now,
    });

    expect(await queue.size()).toBe(3);

    vi.advanceTimersByTime(2100);

    // All three should be dequeuable
    const dequeued1 = await queue.dequeue();
    expect(dequeued1).not.toBeNull();

    const dequeued2 = await queue.dequeue();
    expect(dequeued2).not.toBeNull();

    const dequeued3 = await queue.dequeue();
    expect(dequeued3).not.toBeNull();

    expect(await queue.size()).toBe(0);
  });
});
