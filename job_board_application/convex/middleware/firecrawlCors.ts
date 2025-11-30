import { cors } from "hono/cors";

export const FIRECRAWL_SIGNATURE_HEADER = "X-Firecrawl-Signature";
export const FIRECRAWL_ALLOW_METHODS: string[] = ["POST", "OPTIONS"];

const allowedOrigins = (process.env.FIRECRAWL_WEBHOOK_ORIGINS ?? "")
  .split(",")
  .map((o) => o.trim())
  .filter(Boolean);

const firecrawlCorsMiddleware = cors({
  origin: (origin: string) => (origin && allowedOrigins.includes(origin) ? origin : null),
  allowMethods: FIRECRAWL_ALLOW_METHODS,
  allowHeaders: ["Content-Type", FIRECRAWL_SIGNATURE_HEADER],
  maxAge: 86_400,
});

export const runFirecrawlCors = async (
  request: Request
): Promise<{ headers: Headers; preflight?: Response; originAllowed: boolean }> => {
  const res = new Response(null, { headers: new Headers() });
  const ctx: any = {
    req: {
      header: (name: string) => request.headers.get(name),
      method: request.method,
    },
    res,
    header: (key: string, value: string, opts?: { append?: boolean }) => {
      if (opts?.append) res.headers.append(key, value);
      else res.headers.set(key, value);
    },
  };

  const maybeResponse = await firecrawlCorsMiddleware(ctx, async () => { });
  const originAllowed = !!res.headers.get("Access-Control-Allow-Origin");

  if (maybeResponse instanceof Response) {
    return { headers: maybeResponse.headers, preflight: maybeResponse, originAllowed };
  }

  return { headers: res.headers, originAllowed };
};
