import { describe, expect, it, vi } from "vitest";
import { getHandler } from "./__tests__/getHandler";
import { listScrapeQueue, listFirecrawlWebhooks } from "./router";

vi.mock("@convex-dev/auth/server", () => ({
    getAuthUserId: () => "user-1",
}));

describe("Queued Jobs Tabs", () => {
    it("listScrapeQueue matches backend logic", async () => {
        const handler = getHandler(listScrapeQueue);

        let indexName: string = "";
        let capturedType: any = null;
        let orderDir: string = "";

        const queryBuilder = {
            eq: (field: string, value: any) => {
                if (field === "urlType") capturedType = value;
                return queryBuilder;
            }
        };

        const ctx: any = {
            db: {
                query: (table: string) => {
                    if (table !== "scrape_url_queue") throw new Error("Wrong table");
                    return {
                        withIndex: (name: string, fn: any) => {
                            indexName = name;
                            if (fn) fn(queryBuilder);
                            return {
                                order: (dir: string) => {
                                    orderDir = dir;
                                    return {
                                        take: () => []
                                    };
                                }
                            };
                        }
                    };
                }
            }
        };

        // Test with "listing"
        await handler(ctx, { limit: 10, type: "listing" });
        expect(indexName).toBe("by_urlType");
        expect(capturedType).toBe("listing");
        expect(orderDir).toBe("desc");

        // Test with "detail"
        await handler(ctx, { limit: 10, type: "detail" });
        expect(capturedType).toBe("detail");

        // Test with undefined type (should return empty)
        const emptyResult = await handler(ctx, { limit: 10 });
        expect(emptyResult).toEqual([]);
    });

    it("listFirecrawlWebhooks queries correct index", async () => {
        const handler = getHandler(listFirecrawlWebhooks);
        let indexName = "";
        let orderDir = "";

        const ctx: any = {
            db: {
                query: (table: string) => {
                    if (table !== "firecrawl_webhooks") throw new Error("Wrong table");
                    return {
                        withIndex: (name: string) => {
                            indexName = name;
                            return {
                                order: (dir: string) => {
                                    orderDir = dir;
                                    return {
                                        take: () => []
                                    };
                                }
                            };
                        }
                    };
                }
            }
        };

        await handler(ctx, { limit: 10 });
        expect(indexName).toBe("by_received");
        expect(orderDir).toBe("desc");
    });
});
