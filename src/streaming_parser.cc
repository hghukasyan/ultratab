#include "streaming_parser.h"
#include "pipeline_metrics.h"
#include <cerrno>
#include <chrono>
#include <cstring>
#include <vector>

namespace ultratab {

namespace {

const std::size_t kDefaultReadBufferSize = 256 * 1024;

}  // namespace

StreamingCsvParser::StreamingCsvParser(const std::string& path,
                                       const CsvOptions& options,
                                       std::size_t max_queue_batches,
                                       bool use_mmap,
                                       std::size_t read_buffer_size)
    : path_(path),
      options_(options),
      max_queue_batches_(max_queue_batches > 0 ? max_queue_batches : 2),
      read_buffer_size_(read_buffer_size > 0 ? read_buffer_size : kDefaultReadBufferSize),
      use_mmap_(use_mmap),
      queue_(max_queue_batches_) {
  thread_ = std::thread(&StreamingCsvParser::run, this);
}

StreamingCsvParser::~StreamingCsvParser() {
  stop();
  if (thread_.joinable()) thread_.join();
}

void StreamingCsvParser::stop() {
  stop_requested_.store(true);
  queue_.cancel();
}

void StreamingCsvParser::run() {
  ReaderOptions ropts;
  ropts.use_mmap = use_mmap_;
  ropts.buffer_size = read_buffer_size_;
  FileReader reader(path_, ropts);

  if (reader.hasError()) {
    BatchResult r;
    r.kind = BatchResultKind::Error;
    r.error_message = reader.errorMessage();
    queue_.push(std::move(r));
    return;
  }

  CsvOptions parser_opts = options_;
  SliceCsvParser parser(parser_opts);
  if (profileEnabled()) parser.setMetrics(&metrics_);
  if (options_.has_header) parser.skipOneRow();

  std::vector<char> remainder;
  const char* seg1 = nullptr;
  std::size_t len1 = 0;

  while (!stop_requested_.load()) {
    auto t_read_start = std::chrono::steady_clock::now();
    ByteSpan chunk = reader.getNext();
    auto t_read_end = std::chrono::steady_clock::now();
    if (profileEnabled()) {
      metrics_.read_time_ns.fetch_add(
          static_cast<uint64_t>(std::chrono::duration_cast<std::chrono::nanoseconds>(
              t_read_end - t_read_start).count()));
    }
    if (chunk.empty() && len1 == 0) break;

    auto t_parse_start = std::chrono::steady_clock::now();
    parser.feed(seg1, len1, chunk.data, chunk.size);
    seg1 = nullptr;
    len1 = 0;

    const char* rem_ptr = nullptr;
    std::size_t rem_len = 0;
    parser.getRemainder(&rem_ptr, &rem_len);
    if (rem_ptr && rem_len > 0) {
      remainder.assign(rem_ptr, rem_ptr + rem_len);
      seg1 = remainder.data();
      len1 = remainder.size();
    }

    while (parser.hasBatch()) {
      SliceBatch slice_batch = parser.takeBatch();
      if (profileEnabled()) metrics_.batch_allocations.fetch_add(1);
      auto t_parse_end = std::chrono::steady_clock::now();
      metrics_.parse_time_ns.fetch_add(
          static_cast<uint64_t>(std::chrono::duration_cast<std::chrono::nanoseconds>(
              t_parse_end - t_parse_start).count()));

      auto t_build_start = std::chrono::steady_clock::now();
      Batch batch;
      buildRowBatch(slice_batch, batch);
      auto t_build_end = std::chrono::steady_clock::now();
      if (profileEnabled()) {
        metrics_.build_time_ns.fetch_add(
            static_cast<uint64_t>(std::chrono::duration_cast<std::chrono::nanoseconds>(
                t_build_end - t_build_start).count()));
      }
      metrics_.rows_parsed.fetch_add(batch.size());

      auto t_push_start = std::chrono::steady_clock::now();
      BatchResult result;
      result.kind = BatchResultKind::Batch;
      result.batch = std::move(batch);
      if (!queue_.push(std::move(result))) goto done;
      auto t_push_end = std::chrono::steady_clock::now();
      metrics_.queue_wait_ns.fetch_add(
          static_cast<uint64_t>(std::chrono::duration_cast<std::chrono::nanoseconds>(
              t_push_end - t_push_start).count()));
      if (profileEnabled()) {
        metrics_.emit_time_ns.fetch_add(
            static_cast<uint64_t>(std::chrono::duration_cast<std::chrono::nanoseconds>(
                t_push_end - t_push_start).count()));
      }
      metrics_.batches_emitted.fetch_add(1);
      t_parse_start = std::chrono::steady_clock::now();
    }

    if (chunk.empty()) break;
    metrics_.bytes_read.fetch_add(chunk.size);
  }

  if (seg1 && len1 > 0)
    parser.feed(seg1, len1, nullptr, 0);
  parser.flush();

  while (parser.hasBatch()) {
    SliceBatch slice_batch = parser.takeBatch();
    if (profileEnabled()) metrics_.batch_allocations.fetch_add(1);
    Batch batch;
    auto t_build_start = std::chrono::steady_clock::now();
    buildRowBatch(slice_batch, batch);
    if (profileEnabled()) {
      auto t_build_end = std::chrono::steady_clock::now();
      metrics_.build_time_ns.fetch_add(
          static_cast<uint64_t>(std::chrono::duration_cast<std::chrono::nanoseconds>(
              t_build_end - t_build_start).count()));
    }
    metrics_.rows_parsed.fetch_add(batch.size());
    BatchResult result;
    result.kind = BatchResultKind::Batch;
    result.batch = std::move(batch);
    if (!queue_.push(std::move(result))) goto done;
    metrics_.batches_emitted.fetch_add(1);
  }

  metrics_.bytes_read.store(reader.bytesRead());
  queue_.push(BatchResult{BatchResultKind::Done, Batch{}, ""});

done:
  return;
}

}  // namespace ultratab
