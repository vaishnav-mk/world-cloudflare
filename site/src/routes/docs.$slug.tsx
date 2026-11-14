import { createFileRoute, notFound } from '@tanstack/react-router';
import { DocPage } from '../components/doc-page';
import { docs } from '../content';

export const Route = createFileRoute('/docs/$slug')({
  loader: ({ params }) => {
    const doc = docs.find((doc) => doc.slug === params.slug);
    if (doc === undefined) throw notFound();
    return doc.slug;
  },
  head: ({ loaderData }) => {
    const doc = docs.find((doc) => doc.slug === loaderData);
    return { meta: [{ title: `${doc?.title ?? 'Not found'} · World for Cloudflare` }, ...(doc === undefined ? [] : [{ name: 'description', content: doc.description }])] };
  },
  component: Page,
});

function Page() {
  const slug = Route.useLoaderData();
  const doc = docs.find((doc) => doc.slug === slug);
  if (doc === undefined) throw notFound();
  return <DocPage doc={doc} />;
}
