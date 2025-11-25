#!/usr/bin/env node
/**
 * Capture a PNG of the JobRow fraction rendering using the current snapshot HTML.
 * This runs outside Vitest to avoid worker/headless issues on WSL.
 */

const fs = require("fs");
const path = require("path");
const { chromium } = require("playwright-chromium");

const SNAP_PATH = path.resolve(__dirname, "../src/components/__snapshots__/JobRow.fraction.test.tsx.snap");
const OUT_DIR = path.resolve(__dirname, "../src/components/__screenshots__");
const OUT_FILE = path.join(OUT_DIR, "jobrow-fractions.manual.png");

const snapshot = fs.readFileSync(SNAP_PATH, "utf8");
// Grab the first snapshot block between backticks following " = `"
const marker = " = `";
const start = snapshot.indexOf(marker);
if (start === -1) {
  console.error("Could not find snapshot marker");
  process.exit(1);
}
const end = snapshot.indexOf("`;", start + marker.length);
if (end === -1) {
  console.error("Could not find snapshot terminator");
  process.exit(1);
}
const innerHtml = snapshot.slice(start + marker.length, end);

const pageHtml = `<!DOCTYPE html>
<html>
  <head>
    <style>
      :root { color-scheme: dark; }
      body {
        margin: 0;
        padding: 24px;
        background: #0f172a;
        font-family: 'Inter', system-ui, -apple-system, sans-serif;
      }
    </style>
  </head>
  <body>${innerHtml}</body>
</html>`;

(async () => {
  console.log("Launching Chromium... DISPLAY =", process.env.DISPLAY || "(unset)");
  const browser = await chromium.launch({
    headless: false,
    slowMo: 100,
    args: ["--disable-gpu", "--disable-dev-shm-usage", ...(process.env.DISPLAY ? [`--display=${process.env.DISPLAY}`] : [])],
  });
  const page = await browser.newPage({ viewport: { width: 1400, height: 600 } });
  await page.setContent(pageHtml, { waitUntil: "networkidle" });
  if (!fs.existsSync(OUT_DIR)) fs.mkdirSync(OUT_DIR, { recursive: true });
  await page.screenshot({ path: OUT_FILE, fullPage: true });
  console.log("Saved screenshot to", OUT_FILE);
  await page.waitForTimeout(2000); // keep window up briefly when headed
  await browser.close();
})().catch((err) => {
  console.error(err);
  process.exit(1);
});
