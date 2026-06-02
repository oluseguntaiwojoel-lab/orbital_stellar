import type { RetryQueue, RetryRecord } from "../RetryQueue";

/**
 * In-memory implementation of RetryQueue.
 * Records are stored in a Map for O(1) lookup by ID, and maintained in sorted order by nextRetryAt.
 */
export class InMemoryRetryQueue implements RetryQueue {
  private records: Map<string, RetryRecord> = new Map();
  private sortedByTime: RetryRecord[] = [];

  async enqueue(record: RetryRecord): Promise<void> {
    // Remove if already exists
    if (this.records.has(record.id)) {
      this.sortedByTime = this.sortedByTime.filter((r) => r.id !== record.id);
    }

    // Add to map
    this.records.set(record.id, record);

    // Insert into sorted array maintaining nextRetryAt order
    const insertIdx = this.sortedByTime.findIndex(
      (r) => r.nextRetryAt > record.nextRetryAt,
    );
    if (insertIdx === -1) {
      this.sortedByTime.push(record);
    } else {
      this.sortedByTime.splice(insertIdx, 0, record);
    }
  }

  async dequeue(): Promise<RetryRecord | null> {
    const now = Date.now();

    // First record is earliest due to sort order
    const nextRecord = this.sortedByTime[0];

    // If no records or not ready yet, return null
    if (!nextRecord || nextRecord.nextRetryAt > now) {
      return null;
    }

    // Remove from both storage structures
    this.sortedByTime.shift();
    this.records.delete(nextRecord.id);

    return nextRecord;
  }

  async ack(recordId: string): Promise<void> {
    const record = this.records.get(recordId);
    if (!record) return;

    // Remove from both storage structures
    this.records.delete(recordId);
    this.sortedByTime = this.sortedByTime.filter((r) => r.id !== recordId);
  }

  async clear(): Promise<void> {
    this.records.clear();
    this.sortedByTime = [];
  }

  async size(): Promise<number> {
    return this.records.size;
  }
}
