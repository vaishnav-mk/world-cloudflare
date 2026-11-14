# Architecture docs

TanStack Start, React and Tailwind CSS. The docs compare this World with the public Vercel World. Content lives in `src/content.tsx`; interactive diagrams in `src/components/diagrams.tsx`.

From the repository root:

```sh
pnpm install
pnpm --filter world-cloudflare-docs dev
pnpm --filter world-cloudflare-docs build
pnpm --filter world-cloudflare-docs typecheck
pnpm --filter world-cloudflare-docs test
```

Browser tests use an installed Google Chrome. For CI without Chrome, install Playwright's Chromium and change `channel` in `playwright.config.ts`.

Deploy only this app, not the root workflow example:

```sh
pnpm --filter world-cloudflare-docs build
cd site
pnpm exec wrangler deploy --profile YOUR_PROFILE
```

The site has its own Wrangler config and no bindings to workflow state. TanStack Start renders every documentation route on the server; search, diagram controls, theme and mobile navigation hydrate in the browser. No analytics or server-side search. Fonts load from Google Fonts.
