"use strict";

const { measureRun, summarizeRuns } = require("./metrics");
const config = require("../config");

/**
 * Run warmup iterations, then timed iterations; return aggregated metrics.
 * fn() must return { rowCount, bytesProcessed } (bytesProcessed optional).
 */
async function runBenchmark(name, fn, options = {}) {
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

  const summary = summarizeRuns(runs);
  summary.name = name;
  summary.streaming = options.streaming ?? null;
  return summary;
}

module.exports = { runBenchmark };
