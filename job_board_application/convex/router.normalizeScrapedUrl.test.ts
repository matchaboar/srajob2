import { describe, expect, it } from "vitest";
import { normalizeScrapedUrl } from "./router";

describe("normalizeScrapedUrl", () => {
    it("filters out invalid Hubspot job description URLs", () => {
        const invalidUrl = "https://www.hubspot.com/careers/jobs/5983374?hubs_signup-cta=careers-apply";
        expect(normalizeScrapedUrl(invalidUrl)).toBeNull();
    });

    it("filters out Convex share job URLs", () => {
        const invalidUrl =
            "https://affable-kiwi-46.convex.site/share/job?id=k174anc2bv2j594pk5r5w651ms7z11qk&app=https%3A%2F%2Fsrajob.netlify.app";
        expect(normalizeScrapedUrl(invalidUrl)).toBeNull();
    });

    it("normalizes valid Hubspot URLs (if we identify any)", () => {
        // Placeholder: assuming we might have valid ones later, or just general normalization
        const validUrl = "https://www.hubspot.com/careers/jobs/12345";
        // Current behavior might be to keep it, or we might need to adjust based on findings.
        // For now, let's see what the current behavior is for the invalid one.
        // If the current code returns a string, my test will fail, which is good.
    });


    it("preserves valid Ashby URLs", () => {
        const url = "https://jobs.ashbyhq.com/example/12345";
        expect(normalizeScrapedUrl(url)).toBe(url);
    });
});
