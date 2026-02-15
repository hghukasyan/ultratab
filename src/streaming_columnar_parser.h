#ifndef ULTRATAB_STREAMING_COLUMNAR_PARSER_H
#define ULTRATAB_STREAMING_COLUMNAR_PARSER_H

#include "batch_builder.h"
#include "columnar_parser.h"
#include "csv_parser.h"
#include "pipeline_metrics.h"
#include "reader.h"
#include "ring_queue.h"
#include "slice_parser.h"
#include <atomic>
#include <thread>

namespace ultratab {

enum class ColumnarResultKind { Batch, Done, Cancelled, Error };

struct ColumnarBatchResult {
  ColumnarResultKind kind = ColumnarResultKind::Done;
  ColumnarBatch batch;
  std::string error_message;
};

/// Streaming columnar CSV: Reader → SliceParser → BuildColumnar → RingQueue.
class StreamingColumnarParser {
 public:
  StreamingColumnarParser(const std::string& path,
                          const ColumnarOptions& options,
                          std::size_t max_queue_batches = 2,
                          bool use_mmap = false,
                          std::size_t read_buffer_size = 0);
  ~StreamingColumnarParser();

  StreamingColumnarParser(const StreamingColumnarParser&) = delete;
  StreamingColumnarParser& operator=(const StreamingColumnarParser&) = delete;

  RingQueue<ColumnarBatchResult>& queue() { return queue_; }
  const RingQueue<ColumnarBatchResult>& queue() const { return queue_; }
  const PipelineMetrics& metrics() const { return metrics_; }

  void stop();

 private:
  void run();

  std::string path_;
  ColumnarOptions options_;
  std::size_t max_queue_batches_;
  std::size_t read_buffer_size_;
  bool use_mmap_;
  RingQueue<ColumnarBatchResult> queue_;
  PipelineMetrics metrics_;
  std::thread thread_;
  std::atomic<bool> stop_requested_{false};
};

}  // namespace ultratab

#endif  // ULTRATAB_STREAMING_COLUMNAR_PARSER_H
