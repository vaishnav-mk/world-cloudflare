/** Lets at most `permits` calls of `run` work at once; the others wait their turn in arrival order. */
export class Semaphore {
  #free: number;
  readonly #waiting: (() => void)[] = [];

  constructor(permits: number) {
    this.#free = permits;
  }

  async run<T>(work: () => Promise<T>): Promise<T> {
    if (this.#free > 0) this.#free--;
    else await new Promise<void>((resolve) => this.#waiting.push(resolve));
    try {
      return await work();
    } finally {
      // A waiter takes the permit over, so none is freed in between.
      const next = this.#waiting.shift();
      if (next) next();
      else this.#free++;
    }
  }
}
