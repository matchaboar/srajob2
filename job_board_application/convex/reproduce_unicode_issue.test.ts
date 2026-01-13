import { describe, expect, it } from "vitest";
import { ingestJobsFromScrape } from "./router";
import { getHandler } from "./__tests__/getHandler";

class FakeDb {
    jobs: Map<string, any> = new Map();
    jobDetails: any[] = [];
    jobUrlKeys: any[] = [];
    domainAliases: Map<string, any> = new Map();
    private seq = 1;

    query(table: string) {
        if (table === "jobs") {
            const jobs = this.jobs;
            return {
                withIndex(_name: string, cb: (q: any) => any) {
                    // Mock simple equal match
                    const url = cb({ eq: (_field: string, value: string) => value });
                    const match = Array.from(jobs.values()).find((job) => job.url === url) ?? null;
                    return {
                        first() {
                            return match;
                        },
                    };
                },
            };
        }
        // Minimal mock for other tables
        if (table === "domain_aliases") {
            return {
                withIndex(_name: string, cb: (q: any) => any) {
                    // Mock no aliases found
                    return {
                        first() {
                            return null;
                        },
                    };
                },
            };
        }
        if (table === "job_url_keys") {
            return {
                withIndex(_name: string, cb: (q: any) => any) {
                    return {
                        first() { return null; }
                    }
                }
            }
        }
        return {
            withIndex: () => ({ first: () => null })
        };
    }

    insert(table: string, payload: any) {
        const id = `${table}-${this.seq++}`;
        if (table === "jobs") {
            this.jobs.set(id, { _id: id, ...payload });
            return id;
        }
        return id; // Mock return for other tables
    }

    patch(id: string, payload: any) {
        if (id.startsWith("jobs-")) {
            const existing = this.jobs.get(id);
            this.jobs.set(id, { ...existing, ...payload });
        }
    }

    get(id: string) {
        return null;
    }
}

describe("ingestJobsFromScrape unicode handling", () => {
    it("decodes unicode escape sequences in job titles", async () => {
        const ctx: any = {
            db: new FakeDb(),
            storage: {
                store: async () => "storage-1",
                delete: async () => { },
            },
        };
        const handler = getHandler(ingestJobsFromScrape);
        const now = Date.now();

        await handler(ctx, {
            jobs: [
                {
                    title: "Senior Software Engineer, Guest \\u0026 Host",
                    company: "Airbnb",
                    description: "Role details",
                    location: "San Francisco",
                    remote: false,
                    level: "senior",
                    totalCompensation: 0,
                    url: "https://careers.airbnb.com/positions/123",
                    postedAt: now,
                    postedAtUnknown: false,
                },
            ],
        });

        const job = Array.from(ctx.db.jobs.values())[0];
        expect(job?.title).toBe("Senior Software Engineer, Guest & Host");
    });
});
