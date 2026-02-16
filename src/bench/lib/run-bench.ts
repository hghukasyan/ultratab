"use strict";

const { measureRun, summarizeRuns } = require("./metrics");
const config = require("../config");

interface RunBenchmarkOptions {
  warmup?: number;
  iterations?: number;
  streaming?: boolean;
  measureOptions?: { sampleRssIntervalMs?: number };
}

interface BenchmarkResult {
  rowCount: number;
  bytesProcessed: number;
}

async function runBenchmark(
  name: string,
  fn: () => Promise<BenchmarkResult>,
  options: RunBenchmarkOptions = {}
): Promise<Record<string, unknown>> {
  const warmup = options.warmup ?? config.WARMUP_RUNS;
  const iterations = options.iterations ?? config.ITERATIONS;

  for (let i = 0; i < warmup; i++) {
    await fn();
  }

  const runs = [];
  for (let i = 0; i < iterations; i++) {
    const m = await measureRun(fn, options.measureOptions);
    runs.push(m);
  }

  const summary = summarizeRuns(runs) as Record<string, unknown>;
  summary.name = name;
  summary.streaming = options.streaming ?? null;
  return summary;
}

module.exports = { runBenchmark };
