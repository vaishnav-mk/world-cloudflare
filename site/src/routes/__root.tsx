import { createRootRoute, HeadContent, Outlet, Scripts } from '@tanstack/react-router';
import { DocsLayout } from '../components/layout';
import stylesheet from '../styles.css?url';

export const Route = createRootRoute({
  head: () => ({
    meta: [
      { charSet: 'utf-8' },
      { name: 'viewport', content: 'width=device-width, initial-scale=1' },
      { title: 'World for Cloudflare · Architecture docs' },
      { name: 'description', content: 'How a Workflow SDK World stores, delivers and runs durable workflows on Cloudflare Workers. Architecture, request walkthroughs and a source-based Vercel comparison.' },
      { name: 'theme-color', content: '#f9f8f7' },
    ],
    links: [{ rel: 'stylesheet', href: stylesheet }, { rel: 'icon', href: '/favicon.svg', type: 'image/svg+xml' }],
  }),
  component: Root,
  notFoundComponent: () => <div className="py-20"><h1>Page not found</h1><p>This page does not exist. Choose a topic in the sidebar.</p><a href="/">Go to the introduction</a></div>,
});

function Root() {
  return <html lang="en"><head><HeadContent /></head><body><DocsLayout><Outlet /></DocsLayout><Scripts /></body></html>;
}
