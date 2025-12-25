// @vitest-environment node
import { readFile } from "node:fs/promises";
import path from "node:path";
import { fileURLToPath } from "node:url";
import { describe, expect, it } from "vitest";

const repoRoot = path.resolve(path.dirname(fileURLToPath(import.meta.url)), "..", "..");
const netlifyConfigPath = path.join(repoRoot, "netlify.toml");

const findRedirectBlock = (contents: string, from: string) => {
  const blocks = contents.split("[[redirects]]").slice(1);
  return blocks.find((block) => new RegExp(`from\\s*=\\s*\"${from.replace(/[-/\\^$*+?.()|[\\]{}]/g, "\\\\$&")}\"`).test(block)) ?? null;
};

describe("Netlify share redirects", () => {
  it("routes share endpoints to the Convex HTTP origin", async () => {
    const contents = await readFile(netlifyConfigPath, "utf8");

    const jobShare = findRedirectBlock(contents, "/share/job");
    expect(jobShare).not.toBeNull();
    expect(jobShare).toContain('to = "https://affable-kiwi-46.convex.site/share/job"');
    expect(jobShare).toContain("status = 200");

    const oembed = findRedirectBlock(contents, "/share/job/oembed");
    expect(oembed).not.toBeNull();
    expect(oembed).toContain('to = "https://affable-kiwi-46.convex.site/share/job/oembed"');
    expect(oembed).toContain("status = 200");

    const logo = findRedirectBlock(contents, "/share/jobboard-logo.svg");
    expect(logo).not.toBeNull();
    expect(logo).toContain('to = "https://affable-kiwi-46.convex.site/share/jobboard-logo.svg"');
    expect(logo).toContain("status = 200");
  });
});
