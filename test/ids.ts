const CROCKFORD = '0123456789ABCDEFGHJKMNPQRSTVWXYZ';

/** A `wrun_<ulid>` as `start()` mints it, created at `at`. The World reads a run's creation time from it. */
export function newRunId(at = Date.now()): string {
  let time = '';
  for (let rest = at, index = 0; index < 10; index++, rest = Math.floor(rest / 32)) time = CROCKFORD[rest % 32] + time;
  const random = Array.from(crypto.getRandomValues(new Uint8Array(16)), (byte) => CROCKFORD[byte % 32]).join('');
  return `wrun_${time}${random}`;
}
