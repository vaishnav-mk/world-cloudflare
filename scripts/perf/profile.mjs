// Records a CPU profile of a local `wrangler dev` through its inspector port.
//
//   pnpm exec wrangler dev --port 8811 --inspector-port 9441 ...
//   node scripts/perf/profile.mjs <seconds> <out.cpuprofile> [sampling interval µs=200]
//
// QuickJS runs keep the isolate busy, and the inspector answers only between tasks: start the
// profile before the run and give it enough seconds to outlast it.
import { writeFileSync } from 'node:fs';

const [seconds, out, interval = '200'] = process.argv.slice(2);
const inspector = process.env.INSPECTOR_URL ?? 'http://localhost:9441';
const [target] = await (await fetch(`${inspector}/json`)).json();
// Miniflare's inspector proxy rejects WebSockets without an Origin header.
const socket = new WebSocket(target.webSocketDebuggerUrl, { headers: { origin: 'http://localhost' } });
const pending = new Map();
let nextId = 0;
socket.onmessage = ({ data }) => {
  const message = JSON.parse(data);
  const call = pending.get(message.id);
  if (!call) return;
  pending.delete(message.id);
  if (message.error) call.reject(new Error(message.error.message));
  else call.resolve(message.result);
};
const send = (method, params = {}) => new Promise((resolve, reject) => {
  const id = ++nextId;
  pending.set(id, { resolve, reject });
  socket.send(JSON.stringify({ id, method, params }));
});
await new Promise((resolve, reject) => {
  socket.onopen = resolve;
  socket.onerror = reject;
});

await send('Profiler.enable');
await send('Profiler.setSamplingInterval', { interval: Number(interval) });
await send('Profiler.start');
console.log('started', new Date().toISOString());
await new Promise((resolve) => setTimeout(resolve, Number(seconds) * 1000));
const { profile } = await send('Profiler.stop');
writeFileSync(out, JSON.stringify(profile));
console.log(`wrote ${out}: ${((profile.endTime - profile.startTime) / 1e6).toFixed(1)} s`);
process.exit(0);
