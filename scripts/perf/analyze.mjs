// Compares CPU profiles by function: milliseconds of self and total time per profiled second.
//
//   node scripts/perf/analyze.mjs early.cpuprofile late.cpuprofile [rows=40]
//   node scripts/perf/analyze.mjs run.cpuprofile@3-6 run.cpuprofile@72-86
//
// `@from-to` keeps only the samples between those seconds of the profile. Divide a row by the
// steps per second in its window to get milliseconds per step.
import { readFileSync } from 'node:fs';

const args = process.argv.slice(2);
const rows = Number(args.find((arg) => /^\d+$/.test(arg)) ?? 40);
const specs = args.filter((arg) => arg.includes('.cpuprofile'));
const parsed = new Map();

function load(spec) {
  const [file, range] = spec.split('@');
  if (!parsed.has(file)) parsed.set(file, JSON.parse(readFileSync(file, 'utf8')));
  const { nodes, samples, timeDeltas } = parsed.get(file);
  const [from, to] = range ? range.split('-').map(Number) : [0, Infinity];
  const byId = new Map(nodes.map((node) => [node.id, node]));
  const parent = new Map(nodes.flatMap((node) => (node.children ?? []).map((child) => [child, node.id])));
  const name = ({ callFrame: { functionName, url, lineNumber } }) =>
    `${functionName || '(anon)'} ${url.split('/').at(-1)}:${lineNumber + 1}`;
  const self = new Map();
  const total = new Map();
  let at = 0;
  let covered = 0;
  for (let index = 0; index < samples.length; index++) {
    at += timeDeltas[index];
    const micros = timeDeltas[index + 1] ?? 0;
    if (at / 1e6 < from || at / 1e6 >= to) continue;
    covered += micros;
    const leaf = name(byId.get(samples[index]));
    self.set(leaf, (self.get(leaf) ?? 0) + micros);
    const seen = new Set();
    for (let id = samples[index]; id !== undefined; id = parent.get(id)) {
      const frame = name(byId.get(id));
      if (seen.has(frame)) continue;
      seen.add(frame);
      total.set(frame, (total.get(frame) ?? 0) + micros);
    }
  }
  return { spec: spec.split('/').at(-1), seconds: covered / 1e6, self, total };
}

const profiles = specs.map(load);
for (const kind of ['self', 'total']) {
  console.log(`\n${kind} ms per profiled second: ${profiles.map((p) => `${p.spec} (${p.seconds.toFixed(1)} s)`).join(' | ')}`);
  const value = (profile, frame) => (profile[kind].get(frame) ?? 0) / 1000 / profile.seconds;
  const frames = [...new Set(profiles.flatMap((profile) => [...profile[kind].keys()]))];
  frames.sort((a, b) => value(profiles.at(-1), b) - value(profiles.at(-1), a));
  for (const frame of frames.slice(0, rows)) {
    console.log(`${profiles.map((profile) => value(profile, frame).toFixed(1).padStart(8)).join('')}  ${frame}`);
  }
}
