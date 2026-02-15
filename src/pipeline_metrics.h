#ifndef ULTRATAB_PIPELINE_METRICS_H
#define ULTRATAB_PIPELINE_METRICS_H

#include <cstdint>
#include <atomic>
#include <cstdlib>

namespace ultratab {

/// When ULTRATAB_PROFILE=1 (env) or ULTRATAB_PROFILE is defined, per-stage timings and allocation counts are recorded.
inline bool profileEnabled() {
#ifdef ULTRATAB_PROFILE
  return true;
#else
  const char* v = std::getenv("ULTRATAB_PROFILE");
  return v && (v[0] == '1' || v[0] == 't' || v[0] == 'T');
#endif
}

/// Internal metrics for the producer-consumer pipeline (optional debug exposure).
/// With profiling: read_time_ns, parse_time_ns, build_time_ns, emit_time_ns and allocation counts are populated.
struct PipelineMetrics {
  std::atomic<uint64_t> bytes_read{0};
  std::atomic<uint64_t> rows_parsed{0};
  std::atomic<uint64_t> batches_emitted{0};
  std::atomic<uint64_t> queue_wait_ns{0};
  std::atomic<uint64_t> parse_time_ns{0};

  /// Profiling: time spent in read stage (getNext).
  std::atomic<uint64_t> read_time_ns{0};
  /// Profiling: time spent in build stage (buildRowBatch / buildColumnarBatch).
  std::atomic<uint64_t> build_time_ns{0};
  /// Profiling: time spent waiting to push to queue (emit).
  std::atomic<uint64_t> emit_time_ns{0};
  /// Profiling: arena resize count (slice parser).
  std::atomic<uint64_t> arena_resizes{0};
  /// Profiling: batch allocations (slice batch taken).
  std::atomic<uint64_t> batch_allocations{0};

  /// Arena allocator debug stats (internal).
  std::atomic<uint64_t> arena_bytes_allocated{0};
  std::atomic<uint64_t> arena_blocks{0};
  std::atomic<uint64_t> arena_resets{0};
  std::atomic<uint64_t> peak_arena_usage{0};

  void reset() {
    bytes_read.store(0);
    rows_parsed.store(0);
    batches_emitted.store(0);
    queue_wait_ns.store(0);
    parse_time_ns.store(0);
    read_time_ns.store(0);
    build_time_ns.store(0);
    emit_time_ns.store(0);
    arena_resizes.store(0);
    batch_allocations.store(0);
    arena_bytes_allocated.store(0);
    arena_blocks.store(0);
    arena_resets.store(0);
    peak_arena_usage.store(0);
  }
};

}  // namespace ultratab

#endif  // ULTRATAB_PIPELINE_METRICS_H
