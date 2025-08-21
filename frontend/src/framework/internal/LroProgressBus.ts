export interface ProgressCallback {
    (message: string | null): void;
}

class LroProgressBus {
    private _subscribersMap: Map<string, Set<ProgressCallback>> = new Map();
    private _lastProgressMap: Map<string, string | null> = new Map();
    private _ttlTimers = new Map<string, ReturnType<typeof setTimeout>>();

    subscribe(key: string, callback: ProgressCallback): () => void {
        let subscribersSet = this._subscribersMap.get(key);
        if (!subscribersSet) {
            subscribersSet = new Set();
            this._subscribersMap.set(key, subscribersSet);
        }
        subscribersSet.add(callback);

        // Immediately call the callback with the last known progress message
        const lastProgress = this._lastProgressMap.get(key);
        if (lastProgress !== undefined) {
            queueMicrotask(() => callback(lastProgress));
        }

        return () => {
            const subscribersSet = this._subscribersMap.get(key);
            if (subscribersSet) {
                subscribersSet.delete(callback);
                if (subscribersSet.size === 0) {
                    this._subscribersMap.delete(key);
                }
            }
        };
    }

    publish(key: string, message: string | null): void {
        this._lastProgressMap.set(key, message);
        const subscribersSet = this._subscribersMap.get(key);
        if (!subscribersSet) {
            return;
        }
        for (const callback of subscribersSet) {
            try {
                callback(message);
            } catch (error) {
                console.error(`Error in progress callback for key "${key}":`, error);
            }
        }
    }

    getLast(key: string): string | null | undefined {
        return this._lastProgressMap.get(key);
    }

    remove(key: string): void {
        this._subscribersMap.delete(key);
        this._lastProgressMap.delete(key);
    }

    scheduleRemove(key: string, ttlMs = 60_000) {
        this._ttlTimers.get(key) && clearTimeout(this._ttlTimers.get(key)!);
        const t = setTimeout(() => this.remove(key), ttlMs);
        this._ttlTimers.set(key, t);
    }
}

export const lroProgressBus = new LroProgressBus();

function stableStringify(value: unknown): string {
    const seen = new WeakSet();
    const sorter = (a: any, b: any) => (a > b ? 1 : a < b ? -1 : 0);
    return JSON.stringify(value, function (key, val) {
        if (val && typeof val === "object") {
            if (seen.has(val as object)) return; // drop cycles
            seen.add(val as object);
            if (Array.isArray(val)) return val;
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
