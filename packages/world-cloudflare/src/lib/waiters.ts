/** Long-poll waiters keyed by run or stream name. Lost on eviction, which only ends a poll early. */
export class Waiters {
  readonly #byKey = new Map<string, Set<() => void>>();

  /**
   * Resolves when `wake(key)` is called or after `timeoutMs`. `ready` runs after registering, so a
   * write that landed between the caller's last read and this call still wakes it.
   */
  wait(key: string, timeoutMs: number, ready: () => boolean): Promise<void> {
    return new Promise((resolve) => {
      const waiters = this.#byKey.get(key) ?? new Set<() => void>();
      this.#byKey.set(key, waiters);
      const timer = setTimeout(() => { waiters.delete(wake); resolve(); }, timeoutMs);
      const wake = () => { clearTimeout(timer); waiters.delete(wake); resolve(); };
      waiters.add(wake);
      if (ready()) wake();
    });
  }

  wake(key: string): void {
    for (const wake of this.#byKey.get(key) ?? []) wake();
  }
}
