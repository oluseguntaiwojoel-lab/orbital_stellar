import type { NormalizedEvent, Watcher, WatcherNotification } from "@orbital/pulse-core";
import { createHmac, timingSafeEqual } from "crypto";

import type { VerifyWebhookOptions, WebhookConfig } from "./types.js";
import { DEFAULT_MAX_AGE_MS, DEFAULT_CLOCK_SKEW_MS } from "./types.js";
import type { RetryQueue, RetryRecord } from "./RetryQueue.js";
import { InMemoryRetryQueue } from "./adapters/InMemoryRetryQueue.js";
export { verifyWebhookEdge } from "./edge.js";
export type { VerifyWebhookOptions, WebhookConfig } from "./types.js";
export type { RetryQueue, RetryRecord } from "./RetryQueue.js";

type ResolvedWebhookConfig = Omit<Required<WebhookConfig>, "url"> & {
  urls: string[];
};

export class WebhookDelivery {
  private config: ResolvedWebhookConfig;
  private watcher: Watcher;
  private retryQueue: RetryQueue;
  private queueProcessingInterval: ReturnType<typeof setInterval> | null = null;

  constructor(watcher: Watcher, config: WebhookConfig) {
    this.watcher = watcher;
    this.config = {
      retries: 3,
      deliveryTimeoutMs: 10000,
      maxConcurrentRetries: 100,
      random: Math.random,
      ...config,
      urls: Array.isArray(config.url) ? [...config.url] : [config.url],
    };
    this.config.maxConcurrentRetries = Math.max(1, this.config.maxConcurrentRetries);

    // Initialize retry queue (default to in-memory adapter)
    this.retryQueue = new InMemoryRetryQueue();

    // Start queue processing
    this.queueProcessingInterval = setInterval(() => {
      void this.processRetryQueue();
    }, 100);

    this.watcher.addStopHandler(() => {
      this.stop();
    });

    this.watcher.on("*", (event: NormalizedEvent | WatcherNotification) => {
      if ("raw" in event) {
        for (const url of this.config.urls) {
          void this.deliverToUrl(event, url);
        }
      }
    });
  }

  private async deliverToUrl(
    event: NormalizedEvent,
    url: string,
    attempt = 1,
  ): Promise<void> {
    if (this.watcher.stopped) return;

    const payload = JSON.stringify(event);
    const timestamp = Date.now().toString();
    const signature = this.sign(payload, timestamp);
    const controller = new AbortController();
    const timeoutMs = this.config.deliveryTimeoutMs;
    const abortTimer = setTimeout(() => controller.abort(), timeoutMs);

    try {
      const res = await fetch(url, {
        method: "POST",
        headers: {
          "Content-Type": "application/json",
          "x-orbital-signature": signature,
          "x-orbital-timestamp": timestamp,
          "x-orbital-attempt": String(attempt),
        },
        body: payload,
        signal: controller.signal,
      });

      if (!res.ok) throw new Error(`HTTP ${res.status}`);
    } catch (err) {
      if (this.watcher.stopped) return;

      const errorMessage = this.getErrorMessage(err);

      if (attempt < this.config.retries) {
        // Schedule retry using the queue with nextRetryAt timing
        const exponentialDelay = Math.pow(2, attempt - 1) * 1000;
        const jitteredDelay = Math.floor(this.config.random() * exponentialDelay);
        const nextRetryAt = Date.now() + jitteredDelay;

        const record: RetryRecord = {
          id: `${event.id}-${url}-${attempt + 1}`,
          event,
          url,
          attempt: attempt + 1,
          nextRetryAt,
          createdAt: Date.now(),
        };

        try {
          // Check if we're at capacity; if so, emit dropped and don't enqueue
          if (
            (await this.getQueueSize()) >= this.config.maxConcurrentRetries
          ) {
            this.watcher.emit("webhook.dropped", {
              ...event,
              raw: {
                reason: "retry_cap_exceeded",
                url,
                maxConcurrentRetries: this.config.maxConcurrentRetries,
                originalEvent: event,
              },
            } as unknown as NormalizedEvent);
          } else {
            await this.retryQueue.enqueue(record);
          }
        } catch (queueErr) {
          // If queue operation fails, emit failed event
          this.watcher.emit("webhook.failed", {
            ...event,
            raw: {
              error: this.getErrorMessage(queueErr),
              url,
              attempts: attempt,
              originalEvent: event,
            },
          } as unknown as NormalizedEvent);
        }
      } else {
        this.watcher.emit("webhook.failed", {
          ...event,
          raw: {
            error: errorMessage,
            url,
            attempts: attempt,
            originalEvent: event,
          },
        } as unknown as NormalizedEvent);
      }
    } finally {
      clearTimeout(abortTimer);
    }
  }

  private async processRetryQueue(): Promise<void> {
    if (this.watcher.stopped) return;

    let record: RetryRecord | null;
    while ((record = await this.retryQueue.dequeue()) !== null) {
      // Deliver the retry, which will re-enqueue if needed
      void this.deliverToUrl(record.event, record.url, record.attempt);
    }
  }

  private async getQueueSize(): Promise<number> {
    return this.retryQueue.size();
  }

  private stop(): void {
    // Stop the polling interval
    if (this.queueProcessingInterval !== null) {
      clearInterval(this.queueProcessingInterval);
      this.queueProcessingInterval = null;
    }

    // Clear the queue
    void this.retryQueue.clear();
  }

  private clearRetryTimers(): void {
    // Deprecated - kept for backward compatibility during migration
    // Queue clearing is now handled by stop()
    void this.stop();
  }

  private getErrorMessage(err: unknown): string {
    if (err instanceof Error && err.name === "AbortError") {
      return `Delivery timed out after ${this.config.deliveryTimeoutMs}ms`;
    }

    return err instanceof Error ? err.message : "Unknown error";
  }

  private sign(payload: string, timestamp: string): string {
    const signedPayload = `${timestamp}.${payload}`;

    return createHmac("sha256", this.config.secret)
      .update(signedPayload)
      .digest("hex");
  }
}

export function verifyWebhook(
  payload: string,
  signature: string,
  secret: string,
  timestamp: string,
  options: VerifyWebhookOptions = {},
): NormalizedEvent | null {
  if (!/^\d+$/.test(timestamp)) return null;

  const timestampMs = Number(timestamp);
  if (!Number.isFinite(timestampMs)) return null;

  const maxAgeMs = options.maxAgeMs ?? DEFAULT_MAX_AGE_MS;
  const clockSkewMs = options.clockSkewMs ?? DEFAULT_CLOCK_SKEW_MS;
  const nowMs = options.nowMs ?? Date.now();

  if (timestampMs > nowMs + clockSkewMs) return null;
  if (timestampMs < nowMs - maxAgeMs - clockSkewMs) return null;

  const expected = createHmac("sha256", secret)
    .update(`${timestamp}.${payload}`)
    .digest("hex");

  const expectedBuffer = Buffer.from(expected, "hex");
  const signatureBuffer = Buffer.from(signature, "hex");

  if (expectedBuffer.length !== signatureBuffer.length) return null;
  if (!timingSafeEqual(expectedBuffer, signatureBuffer)) return null;

  try {
    return JSON.parse(payload) as NormalizedEvent;
  } catch {
    return null;
  }
}
