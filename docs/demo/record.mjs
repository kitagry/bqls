// Renders demo.html frame by frame and encodes the frames with ffmpeg.
//
//   node record.mjs            # demo.gif
//   node record.mjs --mp4      # demo.mp4, smoother, for a video attachment
//
// Frames are captured at 2x (1920x1200) so the GIF stays sharp on HiDPI
// screens; the README shows it at width 960.
//
// CHROME_PATH selects a Chrome/Chromium binary instead of the one bundled
// with Playwright (e.g. on NixOS, where the bundled one does not run).
import { spawnSync } from "node:child_process";
import { mkdirSync, rmSync } from "node:fs";
import { dirname, join } from "node:path";
import { fileURLToPath, pathToFileURL } from "node:url";
import { chromium } from "playwright";

const here = dirname(fileURLToPath(import.meta.url));
const mp4 = process.argv.includes("--mp4");
const fps = mp4 ? 30 : 12;
const out = join(here, mp4 ? "demo.mp4" : "demo.gif");
const frames = join(here, "frames");

rmSync(frames, { recursive: true, force: true });
mkdirSync(frames);

const browser = await chromium.launch({ executablePath: process.env.CHROME_PATH || undefined });
const page = await browser.newPage({ viewport: { width: 960, height: 600 }, deviceScaleFactor: 2 });
await page.goto(pathToFileURL(join(here, "demo.html")).href + "?record");
// Web fonts load lazily on first use, so request them before the first frame.
const loaded = await page.evaluate(async () => {
  const specs = ['400 15px "JetBrains Mono"', '700 15px "JetBrains Mono"', "500 13px Inter", "600 12px Inter"];
  const faces = await Promise.all(specs.map((s) => document.fonts.load(s)));
  return faces.every((f) => f.length > 0);
});
if (!loaded) {
  throw new Error("JetBrains Mono did not load; check the network access to Google Fonts");
}

const duration = await page.evaluate(() => window.demo.duration);
const count = Math.ceil((duration / 1000) * fps) + 1;
for (let i = 0; i < count; i++) {
  await page.evaluate((t) => window.demo.render(t), (i * 1000) / fps);
  await page.screenshot({ path: join(frames, `${String(i).padStart(5, "0")}.png`) });
}
await browser.close();

const input = ["-y", "-loglevel", "error", "-framerate", String(fps), "-i", join(frames, "%05d.png")];
const args = mp4
  ? [...input, "-c:v", "libx264", "-pix_fmt", "yuv420p", "-crf", "18", "-movflags", "+faststart", out]
  : [
      ...input,
      // The screen is mostly flat colors, so a per-frame-diff palette without
      // dithering keeps text crisp and the file small.
      "-vf", "split[a][b];[a]palettegen=stats_mode=diff:max_colors=96[p];[b][p]paletteuse=dither=none:diff_mode=rectangle",
      "-loop", "0", out,
    ];
const res = spawnSync("ffmpeg", args, { stdio: "inherit" });
if (res.status !== 0) process.exit(res.status ?? 1);
rmSync(frames, { recursive: true, force: true });
console.log(`wrote ${out} (${count} frames, ${(duration / 1000).toFixed(1)}s)`);
