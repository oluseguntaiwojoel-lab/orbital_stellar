import { createHmac } from "crypto";
import { afterEach, beforeEach, describe, expect, it, vi } from "vitest";

import { Watcher } from "@orbital/pulse-core";
import {
  verifyWebhook,
  verifyWebhookEdge,
  WebhookDelivery,
} from "../src/index.js";

const deliveryEvent = {
  type: "payment.received",
  to: "GDEST",
  from: "GSRC",
  amount: "10",
  asset: "XLM",
  timestamp: "2026-04-26T12:00:00.000Z",
  raw: { id: "evt_1" },
} as const;

async function flushAsyncWork(): Promise<void> {
  await Promise.resolve();
  await Promise.resolve();
}

function signWebhookPayload(
  secret: string,
  payload: string,
  timestamp: string,
): string {
  return createHmac("sha256", secret)
    .update(`${timestamp}.${payload}`)
    .digest("hex");
}

describe("pulse-webhooks WebhookDelivery", () => {
  beforeEach(() => {
    vi.useFakeTimers();
  });

  afterEach(() => {
    vi.useRealTimers();
    vi.unstubAllGlobals();
    vi.restoreAllMocks();
  });

  it("delivers each event to every configured URL", () => {
    vi.setSystemTime(new Date("2026-04-27T00:00:00.000Z"));
    const fetchMock = vi.fn().mockResolvedValue({ ok: true, status: 200 });
    vi.stubGlobal("fetch", fetchMock);

    const secret = "top-secret";
    const payload = JSON.stringify(deliveryEvent);
    const timestamp = Date.now().toString();
    const expectedSignature = signWebhookPayload(secret, payload, timestamp);

    const watcher = new Watcher("GABC");
    new WebhookDelivery(watcher, {
      url: [
        "https://prod.example.com/webhooks/stellar",
        "https://staging.example.com/webhooks/stellar",
        "https://audit.example.com/webhooks/stellar",
      ],
      secret,
    });

    watcher.emit("*", deliveryEvent);

    expect(fetchMock).toHaveBeenCalledTimes(3);
    expect(fetchMock).toHaveBeenNthCalledWith(
      1,
      "https://prod.example.com/webhooks/stellar",
      expect.objectContaining({
        method: "POST",
        body: payload,
        headers: expect.objectContaining({
          "Content-Type": "application/json",
          "x-orbital-attempt": "1",
          "x-orbital-timestamp": timestamp,
          "x-orbital-signature": expectedSignature,
        }),
      }),
    );
    expect(fetchMock).toHaveBeenNthCalledWith(
      2,
      "https://staging.example.com/webhooks/stellar",
      expect.objectContaining({
        method: "POST",
        body: payload,
      }),
    );
    expect(fetchMock).toHaveBeenNthCalledWith(
      3,
      "https://audit.example.com/webhooks/stellar",
      expect.objectContaining({
        method: "POST",
        body: payload,
      }),
    );
  });

  it("keeps delivering to other URLs when one URL fails", async () => {
    const failedUrl = "https://prod.example.com/webhooks/stellar";
    const successfulUrl = "https://audit.example.com/webhooks/stellar";
    const fetchMock = vi.fn((url: string) => {
      if (url === failedUrl) {
        return Promise.resolve({ ok: false, status: 500 });
      }

      return Promise.resolve({ ok: true, status: 200 });
    });
    vi.stubGlobal("fetch", fetchMock);

    const watcher = new Watcher("GABC");
    const failedHandler = vi.fn();
    watcher.on("webhook.failed", failedHandler);

    new WebhookDelivery(watcher, {
      url: [failedUrl, successfulUrl],
      secret: "top-secret",
      retries: 1,
    });

    watcher.emit("*", deliveryEvent);
    await flushAsyncWork();

    expect(fetchMock).toHaveBeenCalledTimes(2);
    expect(fetchMock).toHaveBeenCalledWith(
      failedUrl,
      expect.objectContaining({ method: "POST" }),
    );
    expect(fetchMock).toHaveBeenCalledWith(
      successfulUrl,
      expect.objectContaining({ method: "POST" }),
    );
    expect(failedHandler).toHaveBeenCalledTimes(1);
    expect(failedHandler).toHaveBeenCalledWith(
      expect.objectContaining({
        raw: expect.objectContaining({
          url: failedUrl,
          attempts: 1,
          originalEvent: deliveryEvent,
        }),
      }),
    );
  });

  it("emits webhook.dropped and evicts the newest retry when maxConcurrentRetries cap is reached", async () => {
    const fetchMock = vi.fn().mockRejectedValue(new Error("network down"));
    vi.stubGlobal("fetch", fetchMock);

    const watcher = new Watcher("GABC");
    const droppedHandler = vi.fn();
    watcher.on("webhook.dropped", droppedHandler);

    new WebhookDelivery(watcher, {
      url: "https://example.com/hook",
      secret: "top-secret",
      retries: 3,
      maxConcurrentRetries: 2,
    });

    const event1 = { ...deliveryEvent, raw: { id: "evt_1" } };
    const event2 = { ...deliveryEvent, raw: { id: "evt_2" } };
    const event3 = { ...deliveryEvent, raw: { id: "evt_3" } };

    watcher.emit("*", event1);
    watcher.emit("*", event2);
    watcher.emit("*", event3);
    await flushAsyncWork();

    // events 1 and 2 fill the cap; event 2 (newest) is evicted when event 3's retry is scheduled
    expect(droppedHandler).toHaveBeenCalledTimes(1);
    expect(droppedHandler).toHaveBeenCalledWith(
      expect.objectContaining({
        raw: expect.objectContaining({
          reason: "retry_cap_exceeded",
          url: "https://example.com/hook",
          maxConcurrentRetries: 2,
          originalEvent: expect.objectContaining({ raw: { id: "evt_2" } }),
        }),
      }),
    );
  });

  it("clamps maxConcurrentRetries to 1 when configured as 0 and does not crash", async () => {
    const fetchMock = vi.fn().mockRejectedValue(new Error("network down"));
    vi.stubGlobal("fetch", fetchMock);

    const watcher = new Watcher("GABC");
    const droppedHandler = vi.fn();
    watcher.on("webhook.dropped", droppedHandler);

    new WebhookDelivery(watcher, {
      url: "https://example.com/hook",
      secret: "top-secret",
      retries: 3,
      maxConcurrentRetries: 0,
    });

    watcher.emit("*", { ...deliveryEvent, raw: { id: "evt_1" } });
    watcher.emit("*", { ...deliveryEvent, raw: { id: "evt_2" } });
    await flushAsyncWork();

    // cap is clamped to 1: event 1 fills it, event 2's retry evicts event 1
    expect(droppedHandler).toHaveBeenCalledTimes(1);
    expect(fetchMock).toHaveBeenCalledTimes(2);
  });

  it("cancels pending retries for all URLs when the watcher stops", async () => {
    const fetchMock = vi.fn().mockRejectedValue(new Error("network down"));
    vi.stubGlobal("fetch", fetchMock);

    const watcher = new Watcher("GABC");
    new WebhookDelivery(watcher, {
      url: [
        "https://prod.example.com/webhooks/stellar",
        "https://staging.example.com/webhooks/stellar",
      ],
      secret: "top-secret",
      retries: 3,
    });

    watcher.emit("*", deliveryEvent);
    await flushAsyncWork();

    expect(fetchMock).toHaveBeenCalledTimes(2);

    watcher.stop();
    vi.advanceTimersByTime(10_000);
    await flushAsyncWork();

    expect(fetchMock).toHaveBeenCalledTimes(2);
  });

  it("applies full jitter to retry backoff using a seeded RNG", async () => {
    let seed = 12345;
    const seededRandom = () => {
      seed = (seed * 16807) % 2147483647;
      return (seed - 1) / 2147483646;
    };

    const fetchMock = vi.fn().mockRejectedValue(new Error("network down"));
    vi.stubGlobal("fetch", fetchMock);

    const watcher = new Watcher("GABC");
    const deliveryAttempts: { time: number; attempt: number }[] = [];

    // Track when fetch is called to verify jitter is applied
    const originalFetch = fetch;
    vi.stubGlobal("fetch", (url: string) => {
      deliveryAttempts.push({
        time: Date.now(),
        attempt: deliveryAttempts.length + 1,
      });
      return Promise.reject(new Error("network down"));
    });

    new WebhookDelivery(watcher, {
      url: "https://example.com/webhooks/stellar",
      secret: "top-secret",
      retries: 3,
      random: seededRandom,
    });

    const startTime = Date.now();
    watcher.emit("*", deliveryEvent);
    await flushAsyncWork();

    // Initial attempt
    expect(deliveryAttempts.length).toBe(1);

    // Advance to trigger first retry (should be within jitter bounds for 1st retry: 0-1000ms)
    vi.advanceTimersByTime(600); // Move past the initial jitter delay
    await flushAsyncWork();
    expect(deliveryAttempts.length).toBeGreaterThanOrEqual(2);

    const firstRetryTime = deliveryAttempts[1].time - startTime;
    expect(firstRetryTime).toBeGreaterThanOrEqual(0);
    expect(firstRetryTime).toBeLessThan(1000);
  });

  it("does not dequeue a record whose nextRetryAt is in the future", async () => {
    vi.setSystemTime(new Date("2026-04-27T00:00:00.000Z"));
    const fetchMock = vi.fn().mockRejectedValue(new Error("network down"));
    vi.stubGlobal("fetch", fetchMock);

    const watcher = new Watcher("GABC");
    new WebhookDelivery(watcher, {
      url: "https://example.com/hook",
      secret: "top-secret",
      retries: 2,
      random: () => 0.5, // Use 50% jitter
    });

    watcher.emit("*", deliveryEvent);
    await flushAsyncWork();

    // Initial attempt failed, should enqueue retry with nextRetryAt = now + (2^0 * 1000 * 0.5) = now + 500ms
    expect(fetchMock).toHaveBeenCalledTimes(1);

    // Advance less than 500ms - retry should not have been processed
    vi.advanceTimersByTime(300);
    await flushAsyncWork();

    expect(fetchMock).toHaveBeenCalledTimes(1); // Still only the initial attempt

    // Advance past 500ms - retry should now be processed
    vi.advanceTimersByTime(250); // Total 550ms
    await flushAsyncWork();

    expect(fetchMock).toHaveBeenCalledTimes(2); // Initial + 1 retry
  });

  it("dequeues records in nextRetryAt order regardless of enqueue order", async () => {
    vi.setSystemTime(new Date("2026-04-27T00:00:00.000Z"));
    const deliveryLog: { url: string; attempt: number; time: number }[] = [];

    const fetchMock = vi.fn((url: string) => {
      deliveryLog.push({
        url,
        attempt: deliveryLog.filter((d) => d.url === url).length + 1,
        time: Date.now(),
      });
      return Promise.reject(new Error("network down"));
    });
    vi.stubGlobal("fetch", fetchMock);

    const watcher = new Watcher("GABC");
    let randomCallCount = 0;
    const seededRandom = () => {
      // Generate different jitter values to create different nextRetryAt times
      const values = [0.2, 0.8]; // 200ms and 800ms delays
      return values[randomCallCount++ % values.length];
    };

    new WebhookDelivery(watcher, {
      url: [
        "https://example.com/hook1",
        "https://example.com/hook2",
      ],
      secret: "top-secret",
      retries: 2,
      random: seededRandom,
    });

    watcher.emit("*", { ...deliveryEvent, raw: { id: "evt_1" } });
    await flushAsyncWork();

    // Both URLs have failed initial delivery
    // hook1 will retry at now + 200ms
    // hook2 will retry at now + 800ms

    // Advance to 300ms - only hook1 retry should execute
    vi.advanceTimersByTime(300);
    await flushAsyncWork();

    const hook1Attempt2 = deliveryLog.find((d) => d.url.includes("hook1") && d.attempt === 2);
    const hook2Attempt2 = deliveryLog.find((d) => d.url.includes("hook2") && d.attempt === 2);

    expect(hook1Attempt2).toBeDefined(); // hook1 retry executed
    expect(hook2Attempt2).toBeUndefined(); // hook2 retry not yet executed

    // Advance to 850ms total - now hook2 retry should execute
    vi.advanceTimersByTime(550);
    await flushAsyncWork();

    const hook2Attempt2After = deliveryLog.find(
      (d) => d.url.includes("hook2") && d.attempt === 2,
    );
    expect(hook2Attempt2After).toBeDefined(); // hook2 retry executed
  });

  it("respects a 30-second retry delay for a record enqueued with nextRetryAt = now + 30s", async () => {
    vi.setSystemTime(new Date("2026-04-27T00:00:00.000Z"));
    const fetchMock = vi.fn().mockRejectedValue(new Error("network down"));
    vi.stubGlobal("fetch", fetchMock);

    const watcher = new Watcher("GABC");
    new WebhookDelivery(watcher, {
      url: "https://example.com/hook",
      secret: "top-secret",
      retries: 2,
      random: () => 30_000, // Force 30-second jitter
    });

    watcher.emit("*", deliveryEvent);
    await flushAsyncWork();

    // Initial attempt failed, queued with nextRetryAt = now + 30_000ms
    expect(fetchMock).toHaveBeenCalledTimes(1);

    // Advance 29.9 seconds - should not retry yet
    vi.advanceTimersByTime(29_900);
    await flushAsyncWork();

    expect(fetchMock).toHaveBeenCalledTimes(1); // Still no retry

    // Advance 100ms more (30 seconds total) - retry should execute
    vi.advanceTimersByTime(100);
    await flushAsyncWork();

    expect(fetchMock).toHaveBeenCalledTimes(2); // Initial + 1 retry
  });
});

describe("pulse-webhooks verifyWebhook", () => {
  it("returns parsed event when signature matches timestamped payload", () => {
    const payload = JSON.stringify(deliveryEvent);
    const timestamp = "1714176000000";
    const signature = signWebhookPayload("top-secret", payload, timestamp);

    const event = verifyWebhook(payload, signature, "top-secret", timestamp, {
      nowMs: Number(timestamp),
    });

    expect(event).toEqual(deliveryEvent);
  });

  it("returns null when timestamp is missing or invalid", () => {
    const payload = JSON.stringify(deliveryEvent);
    const signature = signWebhookPayload(
      "top-secret",
      payload,
      "1714176000000",
    );

    expect(verifyWebhook(payload, signature, "top-secret", "")).toBeNull();
    expect(
      verifyWebhook(payload, signature, "top-secret", "not-a-number"),
    ).toBeNull();
  });

  it("returns null when signature does not match timestamped payload", () => {
    const payload = JSON.stringify(deliveryEvent);
    const timestamp = "1714176000000";
    const signature = signWebhookPayload("top-secret", payload, timestamp);

    expect(
      verifyWebhook(payload, signature, "wrong-secret", timestamp),
    ).toBeNull();
    expect(
      verifyWebhook(`${payload}x`, signature, "top-secret", timestamp),
    ).toBeNull();
    expect(
      verifyWebhook(payload, signature, "top-secret", "1714176000001"),
    ).toBeNull();
  });

  it("accepts timestamp within configured clock skew window", () => {
    const payload = JSON.stringify(deliveryEvent);
    const nowMs = 1_714_176_000_000;
    const timestamp = String(nowMs + 20_000);
    const signature = signWebhookPayload("top-secret", payload, timestamp);

    const event = verifyWebhook(payload, signature, "top-secret", timestamp, {
      nowMs,
      maxAgeMs: 60_000,
      clockSkewMs: 30_000,
    });

    expect(event).toEqual(deliveryEvent);
  });

  it("rejects timestamp outside configured skew and maxAge window", () => {
    const payload = JSON.stringify(deliveryEvent);
    const nowMs = 1_714_176_000_000;
    const tooFarFutureTs = String(nowMs + 30_001);
    const tooOldTs = String(nowMs - 60_000 - 30_001);

    const futureSig = signWebhookPayload("top-secret", payload, tooFarFutureTs);
    const oldSig = signWebhookPayload("top-secret", payload, tooOldTs);

    expect(
      verifyWebhook(payload, futureSig, "top-secret", tooFarFutureTs, {
        nowMs,
        maxAgeMs: 60_000,
        clockSkewMs: 30_000,
      }),
    ).toBeNull();
    expect(
      verifyWebhook(payload, oldSig, "top-secret", tooOldTs, {
        nowMs,
        maxAgeMs: 60_000,
        clockSkewMs: 30_000,
      }),
    ).toBeNull();
  });
});

describe("pulse-webhooks verifyWebhookEdge", () => {
  it("returns parsed event when signature matches timestamped payload", async () => {
    const payload = JSON.stringify(deliveryEvent);
    const timestamp = "1714176000000";
    const signature = signWebhookPayload("top-secret", payload, timestamp);

    const event = await verifyWebhookEdge(
      payload,
      signature,
      "top-secret",
      timestamp,
      { nowMs: Number(timestamp) },
    );

    expect(event).toEqual(deliveryEvent);
  });

  it("returns null when timestamp is missing or invalid", async () => {
    const payload = JSON.stringify(deliveryEvent);
    const signature = signWebhookPayload(
      "top-secret",
      payload,
      "1714176000000",
    );

    expect(
      await verifyWebhookEdge(payload, signature, "top-secret", ""),
    ).toBeNull();
    expect(
      await verifyWebhookEdge(payload, signature, "top-secret", "not-a-number"),
    ).toBeNull();
  });

  it("returns null when signature does not match timestamped payload", async () => {
    const payload = JSON.stringify(deliveryEvent);
    const timestamp = "1714176000000";
    const signature = signWebhookPayload("top-secret", payload, timestamp);

    expect(
      await verifyWebhookEdge(payload, signature, "wrong-secret", timestamp),
    ).toBeNull();
    expect(
      await verifyWebhookEdge(
        `${payload}x`,
        signature,
        "top-secret",
        timestamp,
      ),
    ).toBeNull();
    expect(
      await verifyWebhookEdge(
        payload,
        signature,
        "top-secret",
        "1714176000001",
      ),
    ).toBeNull();
  });

  it("accepts timestamp within configured clock skew window", async () => {
    const payload = JSON.stringify(deliveryEvent);
    const nowMs = 1_714_176_000_000;
    const timestamp = String(nowMs + 20_000);
    const signature = signWebhookPayload("top-secret", payload, timestamp);

    const event = await verifyWebhookEdge(payload, signature, "top-secret", timestamp, {
      nowMs,
      maxAgeMs: 60_000,
      clockSkewMs: 30_000,
    });

    expect(event).toEqual(deliveryEvent);
  });

  it("rejects timestamp outside configured skew and maxAge window", async () => {
    const payload = JSON.stringify(deliveryEvent);
    const nowMs = 1_714_176_000_000;
    const tooFarFutureTs = String(nowMs + 30_001);
    const tooOldTs = String(nowMs - 60_000 - 30_001);

    const futureSig = signWebhookPayload("top-secret", payload, tooFarFutureTs);
    const oldSig = signWebhookPayload("top-secret", payload, tooOldTs);

    expect(
      await verifyWebhookEdge(payload, futureSig, "top-secret", tooFarFutureTs, {
        nowMs,
        maxAgeMs: 60_000,
        clockSkewMs: 30_000,
      }),
    ).toBeNull();
    expect(
      await verifyWebhookEdge(payload, oldSig, "top-secret", tooOldTs, {
        nowMs,
        maxAgeMs: 60_000,
        clockSkewMs: 30_000,
      }),
    ).toBeNull();
  });

  it("returns null for malformed JSON payload", async () => {
    const payload = "{ invalid json }";
    const timestamp = "1714176000000";
    const signature = signWebhookPayload("top-secret", payload, timestamp);

    expect(
      await verifyWebhookEdge(payload, signature, "top-secret", timestamp),
    ).toBeNull();
  });
});
