
export type FirecrawlRequest = Request & {
  headers: Headers & {
    get(name: "X-Firecrawl-Signature" | string): string | null;
  };
};

export type FirecrawlWebhookPayload = {
  type?: string;
  id?: string;
  jobId?: string;
  status?: string;
  success?: boolean;
  metadata?: Record<string, any>;
  data?: Record<string, any> | any[];
  error?: string;
  [key: string]: any;
};

export type ParsedFirecrawlResult =
  | { ok: true; body: FirecrawlWebhookPayload; rawText: string; receivedAt: number }
  | { ok: false; status: number; error: string; detail?: string };

export type ParseResult = ParsedFirecrawlResult;

const readRawBody = async (request: Request): Promise<{ buffer: ArrayBuffer; text: string }> => {
  const buffer = await request.arrayBuffer();
  const text = new TextDecoder().decode(buffer);
  return { buffer, text };
};

const toHex = (bytes: ArrayBuffer): string => {
  const arr = new Uint8Array(bytes);
  return Array.from(arr)
    .map((b) => b.toString(16).padStart(2, "0"))
    .join("");
};

export const parseFirecrawlWebhook = async (request: FirecrawlRequest): Promise<ParsedFirecrawlResult> => {
  const signature = request.headers.get("X-Firecrawl-Signature");
  const secret = process.env.FIRECRAWL_WEBHOOK_SECRET;

  if (!signature) {
    return { ok: false, status: 401, error: "Missing signature header" };
  }

  if (!secret) {
    return { ok: false, status: 500, error: "Webhook secret not configured" };
  }

  const [algorithm, hash] = signature.split("=");
  if (algorithm !== "sha256" || !hash) {
    return { ok: false, status: 401, error: "Invalid signature format" };
  }

  const { buffer, text } = await readRawBody(request);

  try {
    const key = await crypto.subtle.importKey(
      "raw",
      new TextEncoder().encode(secret),
      { name: "HMAC", hash: "SHA-256" },
      false,
      ["sign"]
    );
    const expectedBuf = await crypto.subtle.sign("HMAC", key, buffer);
    const expectedHex = toHex(expectedBuf);
    const providedHex = hash.toLowerCase();
    if (expectedHex.length !== providedHex.length || expectedHex !== providedHex) {
      return { ok: false, status: 401, error: "Invalid signature" };
    }
  } catch (err: any) {
    return { ok: false, status: 400, error: "Signature validation failed", detail: err?.message };
  }

  let body: any;
  try {
    body = JSON.parse(text);
  } catch (err: any) {
    return { ok: false, status: 400, error: "Invalid JSON body", detail: err?.message };
  }

  const receivedAt = Date.now();
  return { ok: true, body, rawText: text, receivedAt };
};
