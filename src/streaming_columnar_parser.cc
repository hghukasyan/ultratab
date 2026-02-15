#include "streaming_columnar_parser.h"
#include "pipeline_metrics.h"
#include <cerrno>
#include <chrono>
#include <cstring>
#include <vector>

namespace ultratab {

namespace {

const std::size_t kDefaultReadBufferSize = 256 * 1024;

}  // namespace

StreamingColumnarParser::StreamingColumnarParser(
    const std::string& path, const ColumnarOptions& options,
    std::size_t max_queue_batches, bool use_mmap, std::size_t read_buffer_size)
    : path_(path),
      options_(options),
      max_queue_batches_(max_queue_batches > 0 ? max_queue_batches : 2),
      read_buffer_size_(read_buffer_size > 0 ? read_buffer_size : kDefaultReadBufferSize),
      use_mmap_(use_mmap),
      queue_(max_queue_batches_) {
  thread_ = std::thread(&StreamingColumnarParser::run, this);
}

StreamingColumnarParser::~StreamingColumnarParser() {
  stop();
  if (thread_.joinable()) thread_.join();
}

void StreamingColumnarParser::stop() {
  stop_requested_.store(true);
  queue_.cancel();
}

void StreamingColumnarParser::run() {
  ReaderOptions ropts;
  ropts.use_mmap = use_mmap_;
  ropts.buffer_size = read_buffer_size_;
  FileReader reader(path_, ropts);

  if (reader.hasError()) {
    ColumnarBatchResult r;
    r.kind = ColumnarResultKind::Error;
    r.error_message = reader.errorMessage();
    queue_.push(std::move(r));
    return;
  }

  CsvOptions parser_opts;
  parser_opts.delimiter = options_.delimiter;
  parser_opts.quote = options_.quote;
  parser_opts.has_header = false;
  parser_opts.batch_size = options_.batch_size;

  SliceCsvParser parser(parser_opts);
  if (profileEnabled()) parser.setMetrics(&metrics_);
  // Header row is taken from first batch's first row when has_header is true

  std::vector<std::string> headers;
  std::vector<std::string> selected_headers;
  std::vector<std::size_t> selected_indices;
  bool headers_set = false;
  bool first_data_batch_built = false;
  if (!options_.has_header && !options_.schema.empty()) {
    for (const auto& p : options_.schema) headers.push_back(p.first);
    headers_set = true;
  }

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
      const char* arena = slice_batch.arena.data();
      std::size_t arena_size = slice_batch.arena.size();

      if (!headers_set) {
        if (slice_batch.rows.empty()) break;
        headers = sliceRowToStrings(slice_batch.rows[0], arena, arena_size);
        headers_set = true;
        if (!options_.select.empty()) {
          for (const std::string& name : options_.select) {
            for (std::size_t i = 0; i < headers.size(); ++i) {
              if (headers[i] == name) {
                selected_indices.push_back(i);
                selected_headers.push_back(headers[i]);
                break;
              }
            }
          }
          parser.setSelectedColumnIndices(selected_indices);
        }
        if (slice_batch.rows.size() == 1) {
          ColumnarBatch empty_batch;
          empty_batch.headers = headers;
          empty_batch.rows = 0;
          ColumnarBatchResult result;
          result.kind = ColumnarResultKind::Batch;
          result.batch = std::move(empty_batch);
          if (!queue_.push(std::move(result))) goto done;
          metrics_.batches_emitted.fetch_add(1);
        }
        if (slice_batch.rows.size() <= 1) continue;
        slice_batch.rows.erase(slice_batch.rows.begin());
      }

      if (headers.empty()) {
        ColumnarBatchResult r;
        r.kind = ColumnarResultKind::Error;
        r.error_message = "Could not parse header row";
        queue_.push(std::move(r));
        goto done;
      }

      auto t_build_start = std::chrono::steady_clock::now();
      ColumnarBatch col_batch;
      if (!slice_batch.rows.empty()) {
        const std::vector<std::string>& build_headers =
            (first_data_batch_built && !selected_headers.empty()) ? selected_headers : headers;
        ColumnarOptions build_opts = options_;
        if (first_data_batch_built && !selected_headers.empty()) build_opts.select = selected_headers;
        buildColumnarBatch(slice_batch, build_headers, build_opts, col_batch);
        first_data_batch_built = true;
      } else {
        col_batch.headers = selected_headers.empty() ? headers : selected_headers;
        col_batch.rows = 0;
      }
      auto t_build_end = std::chrono::steady_clock::now();
      if (profileEnabled()) {
        metrics_.build_time_ns.fetch_add(
            static_cast<uint64_t>(std::chrono::duration_cast<std::chrono::nanoseconds>(
                t_build_end - t_build_start).count()));
      }

      metrics_.rows_parsed.fetch_add(col_batch.rows);
      auto t_parse_end = std::chrono::steady_clock::now();
      metrics_.parse_time_ns.fetch_add(
          static_cast<uint64_t>(std::chrono::duration_cast<std::chrono::nanoseconds>(
              t_parse_end - t_parse_start).count()));

      auto t_push_start = std::chrono::steady_clock::now();
      ColumnarBatchResult result;
      result.kind = ColumnarResultKind::Batch;
      result.batch = std::move(col_batch);
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

    if (chunk.empty()) {
      if (seg1 && len1 > 0) parser.feed(seg1, len1, nullptr, 0);
      parser.flush();
      break;
    }
    metrics_.bytes_read.fetch_add(chunk.size);
  }

  if (seg1 && len1 > 0)
    parser.feed(seg1, len1, nullptr, 0);
  parser.flush();

  while (parser.hasBatch()) {
    SliceBatch slice_batch = parser.takeBatch();
    if (profileEnabled()) metrics_.batch_allocations.fetch_add(1);
    if (!headers_set) {
      if (!slice_batch.rows.empty()) {
        headers = sliceRowToStrings(slice_batch.rows[0], slice_batch.arena.data(),
                                    slice_batch.arena.size());
        headers_set = true;
        if (!options_.select.empty()) {
          selected_indices.clear();
          selected_headers.clear();
          for (const std::string& name : options_.select) {
            for (std::size_t i = 0; i < headers.size(); ++i) {
              if (headers[i] == name) {
                selected_indices.push_back(i);
                selected_headers.push_back(headers[i]);
                break;
              }
            }
          }
          parser.setSelectedColumnIndices(selected_indices);
        }
      }
      if (slice_batch.rows.size() <= 1) {
        if (headers_set && slice_batch.rows.size() == 1) {
          ColumnarBatch empty_batch;
          empty_batch.headers = selected_headers.empty() ? headers : selected_headers;
          empty_batch.rows = 0;
          ColumnarBatchResult result;
          result.kind = ColumnarResultKind::Batch;
          result.batch = std::move(empty_batch);
          queue_.push(std::move(result));
        }
        continue;
      }
      slice_batch.rows.erase(slice_batch.rows.begin());
    }
    ColumnarBatch col_batch;
    const std::vector<std::string>& build_headers =
        (first_data_batch_built && !selected_headers.empty()) ? selected_headers : headers;
    ColumnarOptions build_opts = options_;
    if (first_data_batch_built && !selected_headers.empty()) build_opts.select = selected_headers;
    buildColumnarBatch(slice_batch, build_headers, build_opts, col_batch);
    first_data_batch_built = true;
    metrics_.rows_parsed.fetch_add(col_batch.rows);
    ColumnarBatchResult result;
    result.kind = ColumnarResultKind::Batch;
    result.batch = std::move(col_batch);
    if (!queue_.push(std::move(result))) goto done;
    metrics_.batches_emitted.fetch_add(1);
  }

  metrics_.bytes_read.store(reader.bytesRead());

  if (!headers_set && options_.has_header) {
    ColumnarBatchResult r;
    r.kind = ColumnarResultKind::Error;
    r.error_message = "Could not parse header row";
    queue_.push(std::move(r));
  } else {
    queue_.push(ColumnarBatchResult{ColumnarResultKind::Done, ColumnarBatch{}, ""});
  }

done:
  return;
}

}  // namespace ultratab
