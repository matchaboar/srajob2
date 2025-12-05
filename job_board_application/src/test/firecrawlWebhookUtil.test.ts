import crypto from "crypto";
import { describe, expect, it } from "vitest";
import { parseFirecrawlWebhook } from "../../convex/firecrawlWebhookUtil";

const SECRET = "test-secret";

const manualBody = {
  type: "completed",
  id: "manual-test-job-002",
  status: "completed",
  success: true,
  metadata: { siteId: "manual-site-1", siteUrl: "https://example.com/manual" },
  data: { url: "https://example.com/manual", items: [] },
};

const buildRequest = (body: any, headers: Record<string, string> = {}) => {
  const raw = JSON.stringify(body);
  const sig = crypto.createHmac("sha256", SECRET).update(raw).digest("hex");
  const mergedHeaders = new Headers({
    "Content-Type": "application/json",
    "X-Firecrawl-Signature": `sha256=${sig}`,
    ...headers,
  });
  const req = new Request("https://example.com/api/firecrawl/webhook", {
    method: "POST",
    headers: mergedHeaders,
    body: raw,
  });
  return { req, raw };
};

describe("parseFirecrawlRequest", () => {
  it("returns ok for valid signature and json", async () => {
    process.env.FIRECRAWL_WEBHOOK_SECRET = SECRET;
    const body = { type: "completed", id: "job-1", success: true };
    const { req } = buildRequest(body);

    const res = await parseFirecrawlWebhook(req);
    expect(res.ok).toBe(true);
    if (res.ok) {
      expect(res.body.id).toBe("job-1");
      expect(res.rawText).toContain("job-1");
    }
  });

  it("rejects missing signature", async () => {
    process.env.FIRECRAWL_WEBHOOK_SECRET = SECRET;
    const body = { type: "completed" };
    const req = new Request("https://example.com/api/firecrawl/webhook", {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify(body),
    });

    const res = await parseFirecrawlWebhook(req);
    expect(res.ok).toBe(false);
    if (!res.ok) expect(res.status).toBe(401);
  });

  it("rejects invalid signature", async () => {
    process.env.FIRECRAWL_WEBHOOK_SECRET = SECRET;
    const body = { type: "completed", id: "job-1" };
    const { req } = buildRequest(body, { "X-Firecrawl-Signature": "sha256=deadbeef" });

    const res = await parseFirecrawlWebhook(req);
    expect(res.ok).toBe(false);
    if (!res.ok) expect(res.status).toBe(401);
  });

  it("accepts the manual-test-job-002 payload with matching signature", async () => {
    process.env.FIRECRAWL_WEBHOOK_SECRET = SECRET;
    const { req } = buildRequest(manualBody);

    const res = await parseFirecrawlWebhook(req);
    expect(res.ok).toBe(true);
    if (res.ok) {
      expect(res.body.id).toBe("manual-test-job-002");
      expect(res.body.metadata?.siteId).toBe("manual-site-1");
    }
  });

  it("rejects the manual-test-job-002 payload with a bad signature", async () => {
    process.env.FIRECRAWL_WEBHOOK_SECRET = SECRET;
    const raw = JSON.stringify(manualBody);
    const req = new Request("https://example.com/api/firecrawl/webhook", {
      method: "POST",
      headers: {
        "Content-Type": "application/json",
        // intentionally wrong hash
        "X-Firecrawl-Signature": "sha256=0000000000000000000000000000000000000000000000000000000000000000",
      },
      body: raw,
    });

    const res = await parseFirecrawlWebhook(req);
    expect(res.ok).toBe(false);
    if (!res.ok) {
      expect(res.status).toBe(401);
      expect(res.error).toMatch(/Invalid signature/);
    }
  });

  it("rejects invalid json", async () => {
    process.env.FIRECRAWL_WEBHOOK_SECRET = SECRET;
    const raw = "{bad json}";
    const sig = crypto.createHmac("sha256", SECRET).update(raw).digest("hex");
    const req = new Request("https://example.com/api/firecrawl/webhook", {
      method: "POST",
      headers: {
        "X-Firecrawl-Signature": `sha256=${sig}`,
      },
      body: raw,
    });

    const res = await parseFirecrawlWebhook(req);
    expect(res.ok).toBe(false);
    if (!res.ok) {
      expect(res.status).toBe(400);
      expect(res.error).toMatch(/Invalid JSON/);
    }
  });
});
