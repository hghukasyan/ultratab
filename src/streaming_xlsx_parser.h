#ifndef ULTRATAB_STREAMING_XLSX_PARSER_H
#define ULTRATAB_STREAMING_XLSX_PARSER_H

#include "xlsx_parser.h"
#include <atomic>
#include <condition_variable>
#include <mutex>
#include <queue>
#include <thread>

namespace ultratab {

enum class XlsxResultKind { Batch, Done, Cancelled, Error };

struct XlsxBatchResult {
  XlsxResultKind kind = XlsxResultKind::Done;
  XlsxBatch batch;
  std::string error_message;
};

class XlsxBoundedQueue {
 public:
  explicit XlsxBoundedQueue(std::size_t max_size) : max_size_(max_size) {}

  bool push(XlsxBatchResult result);
  bool pop(XlsxBatchResult& out);
  void cancel();

 private:
  std::size_t max_size_;
  std::atomic<bool> cancelled_{false};
  std::queue<XlsxBatchResult> queue_;
  mutable std::mutex mutex_;
  std::condition_variable not_full_;
  std::condition_variable not_empty_;
};

class StreamingXlsxParser {
 public:
  StreamingXlsxParser(const std::string& path, const XlsxOptions& options);
  ~StreamingXlsxParser();

  StreamingXlsxParser(const StreamingXlsxParser&) = delete;
  StreamingXlsxParser& operator=(const StreamingXlsxParser&) = delete;

  XlsxBoundedQueue& queue() { return queue_; }
  const XlsxBoundedQueue& queue() const { return queue_; }

  void stop();

 private:
  void run();

  std::string path_;
  XlsxOptions options_;
  std::size_t max_queue_batches_;
  XlsxBoundedQueue queue_;
  std::thread thread_;
  std::atomic<bool> stop_requested_{false};
};

}  // namespace ultratab

#endif  // ULTRATAB_STREAMING_XLSX_PARSER_H
