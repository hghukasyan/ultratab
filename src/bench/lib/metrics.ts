"use strict";

const { monitorEventLoopDelay } = require("node:perf_hooks");
const config = require("../config");

interface MeasureResult {
  elapsedNs: number;
  elapsedMs: number;
  peakRss: number;
  cpuUserUs: number;
  cpuSystemUs: number;
  eventLoop: { min: number; max: number; mean: number; stddev: number; p50: number; p95: number; count: number };
  rowCount: number;
  bytesProcessed: number;
}

interface MeasureOptions {
  sampleRssIntervalMs?: number;
}

async function measureRun(fn: () => Promise<{ rowCount?: number; rows?: number; bytesProcessed?: number }>, options: MeasureOptions = {}): Promise<MeasureResult> {
  const rssSamples: number[] = [];
  let rssInterval: ReturnType<typeof setInterval> | undefined;

  const cpuBefore = process.cpuUsage();
  const elMonitor = monitorEventLoopDelay({
    resolution: config.EVENT_LOOP_RESOLUTION_MS,
  });
  elMonitor.enable();

  const sampleRssIntervalMs = options.sampleRssIntervalMs ?? 50;
  if (sampleRssIntervalMs > 0) {
    rssInterval = setInterval(() => {
      rssSamples.push(process.memoryUsage().rss);
    }, sampleRssIntervalMs);
  }

  const start = process.hrtime.bigint();
  const result = await fn();
  const end = process.hrtime.bigint();

  if (rssInterval) clearInterval(rssInterval);
  elMonitor.disable();

  const cpuAfter = process.cpuUsage(cpuBefore);
  const rssFinal = process.memoryUsage().rss;
  rssSamples.push(rssFinal);
  const peakRss = rssSamples.length ? Math.max(...rssSamples) : rssFinal;

  const elapsedNs = Number(end - start);
  const elapsedMs = elapsedNs / 1e6;

  return {
    elapsedNs,
    elapsedMs,
    peakRss,
    cpuUserUs: cpuAfter.user,
    cpuSystemUs: cpuAfter.system,
    eventLoop: {
      min: elMonitor.min,
      max: elMonitor.max,
      mean: elMonitor.mean,
      stddev: elMonitor.stddev,
      p50: elMonitor.percentile(50),
      p95: elMonitor.percentile(95),
      count: elMonitor.count,
    },
    rowCount: result?.rowCount ?? result?.rows ?? 0,
    bytesProcessed: result?.bytesProcessed ?? 0,
  };
}

function median(arr: number[]): number {
  if (!arr.length) return 0;
  const s = [...arr].sort((a, b) => a - b);
  const m = Math.floor(s.length / 2);
  return s.length % 2 ? s[m] : (s[m - 1] + s[m]) / 2;
}

function p95(arr: number[]): number {
  if (!arr.length) return 0;
  const s = [...arr].sort((a, b) => a - b);
  const i = Math.ceil(0.95 * s.length) - 1;
  return s[Math.max(0, i)];
}

interface RunSummary {
  medianMs: number;
  p95Ms: number;
  medianPeakRss: number;
  p95PeakRss: number;
  medianEventLoopMax: number;
  p95EventLoopP95: number;
  cpuUserUs: number;
  cpuSystemUs: number;
  rowCount: number;
  bytesProcessed: number;
  runs: number;
}

function summarizeRuns(runs: MeasureResult[]): RunSummary {
  const elapsedMs = runs.map((r) => r.elapsedMs);
  const peakRss = runs.map((r) => r.peakRss);
  const rowCount = runs[0]?.rowCount ?? 0;
  const bytesProcessed = runs[0]?.bytesProcessed ?? 0;

  const elMax = runs.map((r) => r.eventLoop.max);
  const elP95 = runs.map((r) => r.eventLoop.p95);

  return {
    medianMs: median(elapsedMs),
    p95Ms: p95(elapsedMs),
    medianPeakRss: median(peakRss),
    p95PeakRss: p95(peakRss),
    medianEventLoopMax: median(elMax),
    p95EventLoopP95: p95(elP95),
    cpuUserUs: runs.reduce((s, r) => s + r.cpuUserUs, 0) / runs.length,
    cpuSystemUs: runs.reduce((s, r) => s + r.cpuSystemUs, 0) / runs.length,
    rowCount,
    bytesProcessed,
    runs: runs.length,
  };
}

module.exports = {
  measureRun,
  median,
  p95,
  summarizeRuns,
};
