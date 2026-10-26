import type { WorkflowRun } from '@workflow/world';

const KEY_BYTES = 32;
const encoder = new TextEncoder();

/**
 * `getEncryptionKeyForRun` for a Worker secret holding 32 random bytes as base64. Each run's
 * AES-256 key is HKDF-SHA256 of the secret, with a zero salt (the secret is already uniform) and
 * the run ID as info, as the Vercel World derives its keys. Every deployment shares the secret, so
 * the run ID alone picks the key, and a call without the run's deployment still finds it.
 */
export function createGetEncryptionKeyForRun(secret: string): (run: WorkflowRun | string) => Promise<Uint8Array> {
  let material = new Uint8Array();
  try {
    material = Uint8Array.from(atob(secret), (char) => char.charCodeAt(0));
  } catch {
    // Not base64: the length check below reports it.
  }
  if (material.byteLength !== KEY_BYTES) {
    throw new Error(`WORKFLOW_ENCRYPTION_SECRET must be ${KEY_BYTES} random bytes, base64-encoded`);
  }
  return async (run) => {
    const runId = typeof run === 'string' ? run : run.runId;
    const key = await crypto.subtle.importKey('raw', material, 'HKDF', false, ['deriveBits']);
    const info = encoder.encode(`workflow-cloudflare|${runId}`);
    return new Uint8Array(await crypto.subtle.deriveBits(
      { name: 'HKDF', hash: 'SHA-256', salt: new Uint8Array(KEY_BYTES), info }, key, KEY_BYTES * 8,
    ));
  };
}
