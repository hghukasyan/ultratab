#ifndef ULTRATAB_STREAMING_PARSER_H
#define ULTRATAB_STREAMING_PARSER_H

#include "batch_builder.h"
#include "csv_parser.h"
#include "pipeline_metrics.h"
#include "reader.h"
#include "ring_queue.h"
#include "slice_parser.h"
#include <atomic>
#include <memory>
#include <string>
#include <thread>

namespace ultratab {

/// Result for getNextBatch: batch, done, cancelled, or error.
enum class BatchResultKind { Batch, Done, Cancelled, Error };

struct BatchResult {
  BatchResultKind kind = BatchResultKind::Done;
  Batch batch;
  std::string error_message;
};

/// Streaming CSV parser: Reader → SliceParser → BatchBuilder → RingQueue.
/// Bounded queue with backpressure; cancellation stops the worker quickly.
class StreamingCsvParser {
 public:
  StreamingCsvParser(const std::string& path, const CsvOptions& options,
                     std::size_t max_queue_batches = 2,
                     bool use_mmap = false,
                     std::size_t read_buffer_size = 0);
  ~StreamingCsvParser();

  StreamingCsvParser(const StreamingCsvParser&) = delete;
  StreamingCsvParser& operator=(const StreamingCsvParser&) = delete;

  /// Queue of batch results (pop from JS side).
  RingQueue<BatchResult>& queue() { return queue_; }
  const RingQueue<BatchResult>& queue() const { return queue_; }

  /// Internal metrics (optional debug exposure).
  const PipelineMetrics& metrics() const { return metrics_; }

  /// Request parser thread to stop (for early exit).
  void stop();

 private:
  void run();

  std::string path_;
  CsvOptions options_;
  std::size_t max_queue_batches_;
  std::size_t read_buffer_size_;
  bool use_mmap_;
  RingQueue<BatchResult> queue_;
  PipelineMetrics metrics_;
  std::thread thread_;
  std::atomic<bool> stop_requested_{false};
};

}  // namespace ultratab

#endif  // ULTRATAB_STREAMING_PARSER_H
