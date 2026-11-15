import { env } from 'cloudflare:test';
import { afterEach, expect, it, vi } from 'vitest';
import type { WorkflowRun } from '@workflow/world';
import { createWorld } from 'world-cloudflare';
import { handleWorldRpc } from '../packages/world-cloudflare/src/remote/server.js';
import { createWorld as createRemoteWorld } from '../packages/world-cloudflare/src/remote/client.js';

// Bytes 0 to 31.
const SECRET = 'AAECAwQFBgcICQoLDA0ODxAREhMUFRYXGBkaGxwdHh8=';
const RUN_ID = 'wrun_01ARZ3NDEKTSV4RRFFQ69G5FAV';
const hex = (bytes: Uint8Array | undefined) => Buffer.from(bytes ?? []).toString('hex');

afterEach(() => {
  vi.unstubAllGlobals();
});

it('derives a stable 32-byte key per run from the secret, different across runs and secrets', async () => {
  const keyFor = createWorld({ ...env, WORKFLOW_ENCRYPTION_SECRET: SECRET }).getEncryptionKeyForRun!;
  // Pinned: changing the derivation would leave every encrypted run unreadable.
  expect(hex(await keyFor(RUN_ID))).toBe('627e90020d8b8a9cb11e1c53be062a58d0068f8c3fd4a82024c1cce55952bb3e');
  // SAFETY: the derivation reads only the run ID.
  expect(hex(await keyFor({ runId: RUN_ID, deploymentId: 'other' } as WorkflowRun))).toBe(hex(await keyFor(RUN_ID)));
  expect(hex(await keyFor(RUN_ID, { deploymentId: 'other' }))).toBe(hex(await keyFor(RUN_ID)));
  expect(hex(await keyFor('wrun_other'))).not.toBe(hex(await keyFor(RUN_ID)));
  const otherSecret = btoa(String.fromCharCode(...crypto.getRandomValues(new Uint8Array(32))));
  const otherKeyFor = createWorld({ ...env, WORKFLOW_ENCRYPTION_SECRET: otherSecret }).getEncryptionKeyForRun!;
  expect(hex(await otherKeyFor(RUN_ID))).not.toBe(hex(await keyFor(RUN_ID)));
});

it('stays plaintext without a secret and refuses a secret that is not 32 base64 bytes', () => {
  expect(createWorld(env).getEncryptionKeyForRun).toBeUndefined();
  expect(() => createWorld({ ...env, WORKFLOW_ENCRYPTION_SECRET: btoa('short') })).toThrow('32 random bytes');
  expect(() => createWorld({ ...env, WORKFLOW_ENCRYPTION_SECRET: 'not base64!' })).toThrow('32 random bytes');
});

it('serves run keys to the remote client, and none when the Worker has no secret', async () => {
  const remoteFor = (world: ReturnType<typeof createWorld>) => {
    vi.stubGlobal('fetch', (input: string | URL, init?: RequestInit) => handleWorldRpc(new Request(input, init), world, 'token'));
    return createRemoteWorld({ url: 'https://world.test', token: 'token' });
  };
  const encrypted = remoteFor(createWorld({ ...env, WORKFLOW_ENCRYPTION_SECRET: SECRET }));
  expect(hex(await encrypted.getEncryptionKeyForRun!(RUN_ID))).toBe('627e90020d8b8a9cb11e1c53be062a58d0068f8c3fd4a82024c1cce55952bb3e');
  expect(await remoteFor(createWorld(env)).getEncryptionKeyForRun!(RUN_ID)).toBeUndefined();
});
