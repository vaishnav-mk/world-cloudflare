/**
 * Request bodies above this size are rejected with 413. It is the 100 MB request body limit of the
 * Free and Pro Cloudflare plans; Worker memory bounds a usable hook payload well below it.
 */
export const MAX_BODY_BYTES = 100_000_000;

/** Thrown by route handlers; the router turns it into a plain-text response. */
export class HttpError extends Error {
  constructor(readonly status: number, message: string) {
    super(message);
  }
}

/** Parses a JSON body, enforcing `MAX_BODY_BYTES` even without a `content-length`. */
export async function readJson(request: Request): Promise<unknown> {
  if (Number(request.headers.get('content-length') ?? 0) > MAX_BODY_BYTES) throw new HttpError(413, 'Request too large');
  const reader = request.body?.getReader();
  if (!reader) throw new HttpError(400, 'Invalid JSON');
  try {
    const chunks: Uint8Array[] = [];
    let length = 0;
    for (let read = await reader.read(); !read.done; read = await reader.read()) {
      length += read.value.byteLength;
      if (length > MAX_BODY_BYTES) {
        await reader.cancel().catch(() => {});
        throw new HttpError(413, 'Request too large');
      }
      chunks.push(read.value);
    }
    const body = new Uint8Array(length);
    let offset = 0;
    for (const chunk of chunks) {
      body.set(chunk, offset);
      offset += chunk.byteLength;
    }
    return JSON.parse(new TextDecoder('utf-8', { fatal: true }).decode(body));
  } catch (error) {
    throw error instanceof HttpError ? error : new HttpError(400, 'Invalid JSON');
  } finally {
    reader.releaseLock();
  }
}
