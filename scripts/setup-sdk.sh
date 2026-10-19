#!/usr/bin/env bash
set -euo pipefail

root=$(cd "$(dirname "$0")/.." && pwd)
sdk="$root/.vendor/workflow-sdk"
revision=eb9acbc8ba0041e2cbcd06049c29e56568b7ece3
patch="$root/patches/workflow-sdk-cloudflare-workers.patch"

if [[ ! -d "$sdk/.git" ]]; then
  git clone --filter=blob:none --no-checkout https://github.com/vercel/workflow.git "$sdk"
fi
git -C "$sdk" fetch --depth 1 origin "$revision"
git -C "$sdk" checkout --detach "$revision"
if ! git -C "$sdk" apply --reverse --check "$patch" 2>/dev/null; then
  git -C "$sdk" apply --check "$patch"
  git -C "$sdk" apply "$patch"
fi

(cd "$sdk" && pnpm install --dangerously-allow-all-builds && pnpm exec turbo run build --filter='@workflow/cli...' --filter='workflow...')
# The workspace overrides link `workflow` and `@workflow/cli` to the patched checkout.
(cd "$root" && pnpm install --dangerously-allow-all-builds && pnpm run build:workflow)
