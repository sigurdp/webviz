export interface ProgressCallback {
    (message: string | null): void;
}

/**
 * A bus for long-running operation progress messages.
 * It allows any consumer to subscribe to progress messages for a specific serialized query key.
 * To serialize the query key, use the `serializeQueryKey` function.
 * The progress messages are stored in a map, and the bus notifies subscribers when a new message is published.
 * The bus also allows for a TTL (time-to-live) for the messages, after which the messages are removed.
 */
class LroProgressBus {
    private _subscribersMap: Map<string, Set<ProgressCallback>> = new Map();
    private _lastProgressMap: Map<string, string | null> = new Map();

    subscribe(serializedKey: string, callback: ProgressCallback): () => void {
        let subscribersSet = this._subscribersMap.get(serializedKey);
        if (!subscribersSet) {
            subscribersSet = new Set();
            this._subscribersMap.set(serializedKey, subscribersSet);
        }
        subscribersSet.add(callback);

        // Immediately call the callback with the last known progress message
        const lastProgress = this._lastProgressMap.get(serializedKey);
        if (lastProgress !== undefined) {
            queueMicrotask(() => callback(lastProgress));
        }

        return () => {
            const subscribersSet = this._subscribersMap.get(serializedKey);
            if (subscribersSet) {
                subscribersSet.delete(callback);
                if (subscribersSet.size === 0) {
                    this._subscribersMap.delete(serializedKey);
                }
            }
        };
    }

    publish(serializedKey: string, message: string | null): void {
        this._lastProgressMap.set(serializedKey, message);
        const subscribersSet = this._subscribersMap.get(serializedKey);
        if (!subscribersSet) {
            return;
        }
        for (const callback of subscribersSet) {
            try {
                callback(message);
            } catch (error) {
                console.error(`Error in progress callback for key "${serializedKey}":`, error);
            }
        }
    }

    getLast(serializedKey: string): string | null | undefined {
        return this._lastProgressMap.get(serializedKey);
    }

    remove(serializedKey: string): void {
        this._subscribersMap.delete(serializedKey);
        this._lastProgressMap.delete(serializedKey);
    }
}

export const lroProgressBus = new LroProgressBus();

function stableStringify(value: unknown): string {
    const seen = new WeakSet();
    const sorter = (a: any, b: any) => (a > b ? 1 : a < b ? -1 : 0);
    return JSON.stringify(value, function (_, val) {
        if (val && typeof val === "object") {
            if (seen.has(val as object)) {
                return; // drop cycles
            }
            seen.add(val as object);
            if (Array.isArray(val)) {
                return val;
            }
            return Object.keys(val as object)
                .sort(sorter)
                .reduce(
                    (acc, k) => {
                        (acc as any)[k] = (val as any)[k];
                        return acc;
                    },
                    {} as Record<string, unknown>,
                );
        }
        return val;
    });
}

export function serializeQueryKey(key: readonly unknown[]): string {
    return stableStringify(key);
}
