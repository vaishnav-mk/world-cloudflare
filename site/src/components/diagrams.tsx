import { ArrowRight, Check, Copy, RotateCcw } from 'lucide-react';
import { Link } from '@tanstack/react-router';
import { useState } from 'react';
import type { ReactNode } from 'react';

const phases = [
  { title: 'Create', nodes: ['app', 'router', 'store', 'queue'], caption: 'The app calls start(). The store commits run state and durable publish intent; a queue shard accepts the start. Listing updates happen separately.' },
  { title: 'Deliver', nodes: ['queue', 'runner', 'consumer', 'executor'], caption: 'An alarm-driven queue delivery calls a runner. It reaches the consumer, which chooses the executor for the run’s deployment. This is a new request, not a dedicated isolate.' },
  { title: 'Run a step', nodes: ['executor', 'store', 'queue', 'runner'], caption: 'The SDK records step events and runs the body outside the workflow VM. Queued steps use separate delivery capacity; their results wake the orchestrator.' },
  { title: 'Replay', nodes: ['executor', 'store', 'streams'], caption: 'The SDK restores a QuickJS snapshot when available, reads later events and continues. The event log stays authoritative. A completed step’s recorded result replaces executing it again during replay.' },
];
const nodes = [
  { id: 'app', title: 'Your app', sub: 'start()', x: 24, y: 139, w: 116, h: 60, section: 'router' },
  { id: 'router', title: 'Router Worker', sub: 'stateless entry point', x: 180, y: 139, w: 152, h: 60, section: 'router' },
  { id: 'store', title: 'Store shards', sub: 'SQLite · ordered events', x: 180, y: 29, w: 152, h: 62, section: 'store' },
  { id: 'queue', title: 'Queue shards', sub: 'replays / steps', x: 372, y: 139, w: 152, h: 60, section: 'queue' },
  { id: 'runner', title: 'Runners', sub: 'replay & step delivery', x: 564, y: 139, w: 152, h: 60, section: 'runners' },
  { id: 'consumer', title: 'Consumer', sub: 'deployment routing', x: 564, y: 249, w: 152, h: 60, section: 'executors' },
  { id: 'executor', title: 'Executor Worker', sub: 'SDK + workflow bundle', x: 372, y: 249, w: 152, h: 60, section: 'executors' },
  { id: 'streams', title: 'Run streams + R2', sub: 'bytes / snapshots', x: 372, y: 29, w: 152, h: 62, section: 'streams' },
  { id: 'index', title: 'Index & token shards', sub: 'lists / hook lookup', x: 564, y: 29, w: 152, h: 62, section: 'indexes' },
];
const edges = [
  { from: 'app', to: 'router', d: 'M140 169H180' },
  { from: 'router', to: 'store', d: 'M256 139V91' },
  { from: 'store', to: 'queue', d: 'M332 60H350V169H372' },
  { from: 'queue', to: 'runner', d: 'M524 169H564' },
  { from: 'runner', to: 'consumer', d: 'M640 199V249' },
  { from: 'consumer', to: 'executor', d: 'M564 279H524' },
  { from: 'executor', to: 'store', d: 'M372 279H256V199' },
  { from: 'executor', to: 'streams', d: 'M448 249V199M448 139V91' },
  { from: 'store', to: 'index', d: 'M332 45H352V15H640V29' },
];

export function ArchitectureDiagram() {
  const [phase, setPhase] = useState(0);
  const active = phases[phase];
  return <figure className="diagram-card">
    <div className="diagram-top"><span className="status">One run, on Cloudflare</span><span className="max-md:hidden">Click a service to explore it</span><span className="md:hidden">Swipe to explore →</span></div>
    <div className="diagram-scroll"><svg className="architecture-svg" viewBox="0 0 740 339" role="img" aria-labelledby="architecture-title architecture-description">
      <title id="architecture-title">The Cloudflare World’s request and execution paths</title>
      <desc id="architecture-description">The app reaches a router, which writes to a store shard. Store outbox messages reach queue shards. Runners call the consumer and selected executor. The executor reads and writes run state, streams and snapshots.</desc>
      {edges.map((edge) => <path key={`${edge.from}-${edge.to}`} className="edge" d={edge.d} data-hot={active.nodes.includes(edge.from) && active.nodes.includes(edge.to)} />)}
      {nodes.map((node) => <Link key={node.id} to="/docs/$slug" params={{ slug: 'services' }} hash={node.section} className="node-link" aria-label={`Read about ${node.title}`}>
        <g data-hot={active.nodes.includes(node.id)}><rect className="node-box" x={node.x} y={node.y} width={node.w} height={node.h} rx="9" />
          <text className="node-title" x={node.x + 13} y={node.y + 25}>{node.title}</text><text className="node-sub" x={node.x + 13} y={node.y + 44}>{node.sub}</text></g>
      </Link>)}
    </svg></div>
    <div className="diagram-tabs" aria-label="Highlight an execution phase">{phases.map((item, index) => <button key={item.title} type="button" aria-pressed={index === phase} onClick={() => setPhase(index)}>{item.title}</button>)}</div>
    <figcaption className="diagram-caption" aria-live="polite">{active.caption}</figcaption>
    <div className="diagram-legend"><span><i className="line-key" />Selected path</span><span><i className="line-key gray" />Other calls</span><span>Logical components, not individual machines</span></div>
  </figure>;
}

export function Flow({ items }: { items: string[] }) {
  return <div className="arrow-line" role="list">{items.map((item, index) => <div className="contents" key={item}>{index > 0 && <ArrowRight className="flow-arrow size-4 shrink-0" aria-hidden="true" />}<div className="flow-box" role="listitem">{item}</div></div>)}</div>;
}

export function ReplayDiagram() {
  const [shown, setShown] = useState(false);
  return <figure className="diagram-card"><div className="diagram-top"><span className="status">Replay work, illustrated</span><button className="plain-button flex items-center gap-2" onClick={() => setShown(!shown)}><RotateCcw size={12} />{shown ? 'Reset' : 'Show reads'}</button></div><div className="replay-demo">
    <div className="replay-heading">QuickJS<span>Restore snapshot + read new events</span></div>
    <div className="tape" aria-label="QuickJS restores the first 16 blocks and reads 4 tail blocks">{Array.from({ length: 20 }, (_, index) => <span key={index} className={index === 15 ? 'snapshot' : shown && index > 15 ? 'read' : ''} />)}</div>
    <p className="note">Snapshot boundary ↑. Earlier decisions are already in the saved VM.</p>
    <div className="replay-heading mt-6">Native engine<span>Replay recorded history</span></div>
    <div className="tape" aria-label="Native replay reads all 20 blocks">{Array.from({ length: 20 }, (_, index) => <span key={index} className={shown ? 'read' : ''} />)}</div>
    <p className="note">The body of a completed step is not rerun. Core supplies its saved result.</p>
    </div><figcaption className="bar-caption">Conceptual work, not a timing scale. Snapshot availability and compatibility determine whether restore is possible.</figcaption></figure>;
}

export function EpochDiagram() {
  const [size, setSize] = useState(16);
  const oldShard = 13;
  const newShard = 109 % size;
  return <figure className="diagram-card"><div className="diagram-top"><span className="status">Routing without moving old runs</span><span>Illustrative hash = 109</span></div><div className="epoch-demo"><div className="epoch-controls"><label htmlFor="shards">New epoch: <strong>{size} store shards</strong></label><select id="shards" className="rounded-md border border-line bg-card px-3 py-1 text-xs" value={size} onChange={(event) => setSize(Number(event.target.value))}>{[16, 32, 64].map((count) => <option key={count} value={count}>{count} shards</option>)}</select></div>
      <div className="shard-grid" aria-label={`New run routes to shard ${newShard}`}>{Array.from({ length: size }, (_, index) => <span key={index} className={`shard-cell${index === newShard ? ' selected' : ''}`}>{index}</span>)}</div><div className="epoch-caption">Old run → epoch A → <code>109 % 16 = {oldShard}</code>, unchanged.<br />New run → epoch B → <code>109 % {size} = {newShard}</code>.</div></div><figcaption className="bar-caption">The ULID time selects the epoch first. Changing a historical epoch would route old runs to the wrong objects.</figcaption></figure>;
}

export function Code({ children, label = 'Example' }: { children: string; label?: string }) {
  const [copied, setCopied] = useState(false);
  const [failed, setFailed] = useState(false);
  return <div className="code-block"><div className="flex items-center justify-between border-b border-line px-4 py-2 text-[10px] text-muted"><span>{label}</span><button type="button" aria-label={copied ? 'Copied' : 'Copy code'} className="inline-flex items-center gap-1.5 rounded px-1 py-0.5 hover:text-ink" onClick={async () => {
    try { await navigator.clipboard.writeText(children); setCopied(true); setFailed(false); } catch { setFailed(true); }
  }}>{copied ? <Check size={12} /> : <Copy size={12} />}{failed ? 'Select and copy below' : copied ? 'Copied' : 'Copy'}</button></div><pre tabIndex={0}><code>{children}</code></pre></div>;
}

export function Note({ children }: { children: ReactNode }) { return <aside className="notice">{children}</aside>; }

export function Table({ headers, rows }: { headers: string[]; rows: ReactNode[][] }) {
  return <div className="table-wrap" tabIndex={0} role="region" aria-label={headers.join(', ')}><table><thead><tr>{headers.map((header) => <th key={header} scope="col">{header}</th>)}</tr></thead><tbody>{rows.map((row, index) => <tr key={index}>{row.map((cell, cellIndex) => cellIndex === 0 ? <th key={cellIndex} scope="row">{cell}</th> : <td key={cellIndex}>{cell}</td>)}</tr>)}</tbody></table></div>;
}
