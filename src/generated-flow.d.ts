declare module '*.mjs' {
  export const POST: (request: Request) => Promise<Response>;
}
