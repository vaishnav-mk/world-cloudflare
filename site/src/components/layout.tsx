import { Link, useLocation } from '@tanstack/react-router';
import { ChevronRight, CodeXml, Menu, Moon, Search, Sun, X } from 'lucide-react';
import { useEffect, useRef, useState } from 'react';
import type { ReactNode } from 'react';
import { docs } from '../content';

const groups = [...new Set(docs.map((doc) => doc.group))];

function Navigation({ onNavigate }: { onNavigate?: () => void }) {
  const location = useLocation();
  return <nav aria-label="Documentation">{groups.map((group) => <div className="mb-7" key={group}><p className="mb-2 px-3 text-xs font-medium text-muted">{group}</p><ul className="m-0 list-none p-0">{docs.filter((doc) => doc.group === group).map((doc) => {
    const current = doc.slug === 'introduction' ? location.pathname === '/' : location.pathname === `/docs/${doc.slug}`;
    const Icon = doc.icon;
    const style = `my-0.5 flex items-center gap-2.5 rounded-lg border px-3 py-1.5 text-[13px] leading-snug no-underline transition-colors ${current ? 'border-orange/40 bg-tint text-deep' : 'border-transparent text-soft hover:bg-wash hover:text-ink'}`;
    const children = <><Icon className="size-4 shrink-0" /><span>{doc.slug === 'introduction' ? 'Introduction' : doc.title}</span></>;
    return <li className="m-0 p-0" key={doc.slug}>{doc.slug === 'introduction' ? <Link to="/" className={style} aria-current={current ? 'page' : undefined} onClick={onNavigate}>{children}</Link> : <Link to="/docs/$slug" params={{ slug: doc.slug }} className={style} aria-current={current ? 'page' : undefined} onClick={onNavigate}>{children}</Link>}</li>;
  })}</ul></div>)}</nav>;
}

export function DocsLayout({ children }: { children: ReactNode }) {
  const location = useLocation();
  const doc = docs.find((item) => location.pathname === `/docs/${item.slug}`) ?? docs[0];
  const search = useRef<HTMLDialogElement>(null);
  const menu = useRef<HTMLDialogElement>(null);
  const [query, setQuery] = useState('');
  const [dark, setDark] = useState(false);
  const [active, setActive] = useState('');
  const results = docs.flatMap((item) => [
    { slug: item.slug, title: item.slug === 'introduction' ? 'Introduction' : item.title, section: '', context: item.description, match: `${item.title} ${item.description} ${item.keywords}` },
    ...item.sections.map((section) => ({ slug: item.slug, title: section.title, section: section.id, context: item.title, match: `${section.title} ${item.keywords}` })),
  ]).filter((item) => query.trim().toLowerCase().split(/\s+/).every((word) => item.match.toLowerCase().includes(word)));

  useEffect(() => {
    let preference = window.matchMedia('(prefers-color-scheme: dark)').matches;
    try { const stored = localStorage.getItem('world-theme'); if (stored !== null) preference = stored === 'dark'; } catch { /* Theme still works without storage. */ }
    setDark(preference);
    document.documentElement.dataset.theme = preference ? 'dark' : 'light';
    const keyboard = (event: KeyboardEvent) => {
      if ((event.metaKey || event.ctrlKey) && event.key.toLowerCase() === 'k') { event.preventDefault(); setQuery(''); search.current?.showModal(); }
    };
    document.addEventListener('keydown', keyboard);
    return () => document.removeEventListener('keydown', keyboard);
  }, []);

  useEffect(() => {
    setActive('');
    const observer = new IntersectionObserver((entries) => {
      const visible = entries.find((entry) => entry.isIntersecting);
      if (visible !== undefined) setActive(visible.target.id);
    }, { rootMargin: '-85px 0px -60% 0px' });
    document.querySelectorAll('article section[id]').forEach((section) => observer.observe(section));
    return () => observer.disconnect();
  }, [location.pathname]);

  return <>
    <a className="skip" href="#main">Skip to content</a>
    <header className="sticky top-0 z-30 h-[72px] border-b border-line bg-paper/95 backdrop-blur-md max-md:h-[62px]">
      <div className="mx-auto flex h-full max-w-[1536px] items-center gap-9 px-10 max-lg:px-6 max-md:gap-3 max-md:px-[18px]">
        <Link to="/" className="flex shrink-0 items-center gap-2.5 text-base font-semibold tracking-[-.6px] no-underline max-md:gap-2 max-md:text-sm" aria-label="World for Cloudflare home"><svg viewBox="0 0 28 28" className="size-7 max-md:size-6" fill="none" aria-hidden="true"><rect x="2" y="3" width="9" height="22" rx="3" stroke="currentColor" strokeWidth="1.5" /><rect x="17" y="3" width="9" height="22" rx="3" stroke="var(--orange)" strokeWidth="1.5" /><path d="M11 14h6M4.5 9h4M19.5 19h4" stroke="var(--orange)" strokeWidth="1.5" /></svg><span>World<span className="font-normal text-deep"> / Cloudflare</span></span></Link>
        <div className="flex items-center gap-6 text-[13px] text-muted max-lg:hidden"><Link to="/" className="text-ink no-underline">Documentation</Link><Link to="/docs/$slug" params={{ slug: 'architecture' }} className="no-underline">Architecture</Link><Link to="/docs/$slug" params={{ slug: 'operations' }} className="no-underline">Run it</Link></div>
        <div className="ml-auto flex items-center gap-4 max-md:gap-1">
          <button className="flex min-w-[166px] items-center gap-2 rounded-lg border border-line bg-card px-2.5 py-1.5 text-xs text-muted hover:border-orange/40 max-md:min-w-0 max-md:border-0 max-md:bg-transparent max-md:p-2" onClick={() => { setQuery(''); search.current?.showModal(); }} aria-label="Search documentation"><Search size={14} /><span className="max-md:hidden">Search docs</span><kbd className="ml-auto rounded border border-line px-1 text-[10px] max-md:hidden">⌘ K</kbd></button>
          <a href="https://github.com/vaishnav-mk/world-cloudflare" className="flex items-center gap-1.5 text-xs no-underline max-md:hidden"><CodeXml size={15} /><span>GitHub</span></a>
          <button className="rounded-md p-2 text-soft hover:bg-wash" aria-label={dark ? 'Use light theme' : 'Use dark theme'} onClick={() => { const next = !dark; setDark(next); document.documentElement.dataset.theme = next ? 'dark' : 'light'; try { localStorage.setItem('world-theme', next ? 'dark' : 'light'); } catch { /* Optional preference storage. */ } }}>{dark ? <Sun size={15} /> : <Moon size={15} />}</button>
          <button className="rounded-md p-2 md:hidden" aria-label="Open navigation" onClick={() => menu.current?.showModal()}><Menu size={18} /></button>
        </div>
      </div>
    </header>
    <div className="mx-auto grid max-w-[1536px] grid-cols-[216px_minmax(0,808px)_172px] items-start justify-center gap-14 px-10 max-[1250px]:grid-cols-[205px_minmax(0,808px)] max-[1250px]:gap-10 max-lg:grid-cols-[180px_minmax(0,1fr)] max-lg:gap-6 max-lg:px-6 max-md:block max-md:px-[22px]">
      <aside className="sticky top-[72px] max-h-[calc(100dvh-72px)] overflow-y-auto py-9 [scrollbar-width:thin] max-md:hidden"><Navigation /><div className="mx-3 border-t border-line pt-4 text-[11px] leading-relaxed text-muted"><span className="inline-flex items-center gap-1.5"><span className="size-1.5 rounded-full bg-orange" /> Workflow SDK v5</span><p className="mt-2! text-[11px]!">Architecture and implementation notes.<br />Not a managed service.</p><Link to="/docs/$slug" params={{ slug: 'benchmarks' }} className="mt-2 block text-[11px]">Measurements and caveats ↗</Link></div></aside>
      <main id="main" className="min-w-0 pt-9 pb-16 outline-none max-md:pt-7" tabIndex={-1}>{children}</main>
      <aside className="sticky top-[72px] max-h-[calc(100dvh-72px)] overflow-y-auto py-10 max-[1250px]:hidden" aria-label="On this page"><p className="mb-3 text-xs font-medium text-muted">On this page</p><ul className="m-0 list-none border-l border-line p-0">{doc.sections.map((section) => <li key={section.id} className="m-0 p-0"><a href={`#${section.id}`} className={`-ml-px block border-l py-1 pl-3 text-xs leading-snug no-underline ${active === section.id ? 'border-orange text-deep' : 'border-transparent text-muted hover:text-ink'}`} aria-current={active === section.id ? 'location' : undefined}>{section.title}</a></li>)}</ul><div className="mt-6 border-t border-line pt-4 text-[11px] text-muted"><p className="text-[11px]!">Read the implementation</p><a href="https://github.com/vaishnav-mk/world-cloudflare" className="flex items-center gap-1.5 no-underline"><CodeXml size={12} /> Repository <ChevronRight size={11} /></a></div></aside>
    </div>
    <dialog ref={search} className="search-dialog" aria-labelledby="search-title" onKeyDown={(event) => { if (event.key === 'Escape') { event.preventDefault(); search.current?.close(); } }} onClick={(event) => { if (event.target === event.currentTarget) search.current?.close(); }}>
      <div className="flex items-center gap-3 border-b border-line px-5 py-3"><Search size={17} className="shrink-0 text-muted" /><label id="search-title" htmlFor="docs-search" className="sr-only">Search documentation</label><input id="docs-search" type="search" autoFocus autoComplete="off" placeholder="Search services, concepts, limits..." value={query} onChange={(event) => setQuery(event.target.value)} className="min-w-0 flex-1 border-0 bg-transparent text-sm text-ink outline-none" /><button aria-label="Close search" onClick={() => search.current?.close()} className="rounded p-1 text-muted"><X size={15} /></button></div>
      <div className="max-h-[50dvh] overflow-y-auto p-2" aria-live="polite"><p className="px-3 py-1 text-[10px] text-muted">{query.trim() === '' ? 'Browse topics or type to search' : `${results.length} matching topics`}</p>{results.length === 0 && <p className="px-3 py-3 text-sm">No matching topic. Try “queue”, “snapshot” or “payload”.</p>}{results.map((result) => <Link key={`${result.slug}-${result.section}`} to={result.slug === 'introduction' ? '/' : '/docs/$slug'} params={{ slug: result.slug }} hash={result.section} onClick={() => search.current?.close()} className="block rounded-md px-3 py-2.5 text-[13px] no-underline hover:bg-wash focus:bg-wash"><span className="flex items-center justify-between gap-3">{result.title}<ChevronRight size={12} className="shrink-0 text-muted" /></span><span className="mt-1 block text-[11px] leading-relaxed text-muted">{result.context}</span></Link>)}</div><div className="flex justify-between border-t border-line px-5 py-2 text-[10px] text-muted"><span>Local search. Nothing sent to a server.</span><span>Esc to close</span></div>
    </dialog>
    <dialog ref={menu} className="mobile-dialog" aria-labelledby="navigation-title"><div className="flex items-center justify-between px-3 pb-4"><span id="navigation-title" className="text-sm font-medium">Documentation</span><button aria-label="Close navigation" onClick={() => menu.current?.close()} className="p-1"><X size={17} /></button></div><Navigation onNavigate={() => menu.current?.close()} /></dialog>
  </>;
}
