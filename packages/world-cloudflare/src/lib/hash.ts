const encoder = new TextEncoder();

export async function sha256Hex(text: string): Promise<string> {
  const digest = await crypto.subtle.digest('SHA-256', encoder.encode(text));
  return Buffer.from(digest).toString('hex');
}

/** Queue message IDs are derived from the idempotency key, so a retried publish reuses the same ID. */
export async function messageIdFor(key: string): Promise<string> {
  return `qmsg_${await sha256Hex(key)}`;
}

/** 32-bit FNV-1a, as the SDK uses for step idempotency keys. */
export function fnv1a32Hex(text: string): string {
  let hash = 0x811c9dc5;
  for (let i = 0; i < text.length; i++) {
    hash ^= text.charCodeAt(i);
    hash = Math.imul(hash, 0x01000193);
  }
  return (hash >>> 0).toString(16).padStart(8, '0');
}
