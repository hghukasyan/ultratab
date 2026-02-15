#include "streaming_xlsx_parser.h"
#include <cerrno>
#include <cstring>

extern "C" {
#include "miniz.h"
#include "miniz_zip.h"
}

namespace ultratab {

namespace {

const std::size_t kMaxQueueBatches = 2;

}  // namespace

bool XlsxBoundedQueue::push(XlsxBatchResult result) {
  std::unique_lock<std::mutex> lock(mutex_);
  not_full_.wait(lock, [this] {
    return cancelled_.load() || queue_.size() < max_size_;
  });
  if (cancelled_.load()) return false;
  queue_.push(std::move(result));
  not_empty_.notify_one();
  return true;
}

bool XlsxBoundedQueue::pop(XlsxBatchResult& out) {
  std::unique_lock<std::mutex> lock(mutex_);
  not_empty_.wait(lock, [this] { return cancelled_.load() || !queue_.empty(); });
  if (cancelled_.load()) return false;
  out = std::move(queue_.front());
  queue_.pop();
  not_full_.notify_one();
  return true;
}

void XlsxBoundedQueue::cancel() {
  cancelled_.store(true);
  not_full_.notify_all();
  not_empty_.notify_all();
}

StreamingXlsxParser::StreamingXlsxParser(
    const std::string& path, const XlsxOptions& options)
    : path_(path),
      options_(options),
      max_queue_batches_(kMaxQueueBatches),
      queue_(max_queue_batches_) {
  thread_ = std::thread(&StreamingXlsxParser::run, this);
}

StreamingXlsxParser::~StreamingXlsxParser() {
  stop();
  if (thread_.joinable()) thread_.join();
}

void StreamingXlsxParser::stop() {
  stop_requested_.store(true);
  queue_.cancel();
}

void StreamingXlsxParser::run() {
  try {
  mz_zip_archive zip;
  mz_zip_zero_struct(&zip);
  if (!mz_zip_reader_init_file(&zip, path_.c_str(), 0)) {
    XlsxBatchResult r;
    r.kind = XlsxResultKind::Error;
    r.error_message = "Failed to open XLSX (ZIP): ";
    r.error_message += path_;
#ifdef _WIN32
    r.error_message += " (errno ";
    r.error_message += std::to_string(errno);
    r.error_message += ")";
#else
    r.error_message += std::strerror(errno);
#endif
    queue_.push(std::move(r));
    return;
  }

  std::string err;
  auto [shared_strings, sheet_path] = xlsxResolveSheetFromZip(
      &zip,
      options_.sheet_index,
      options_.sheet_name,
      err);
  if (!err.empty()) {
    mz_zip_reader_end(&zip);
    XlsxBatchResult r;
    r.kind = XlsxResultKind::Error;
    r.error_message = std::move(err);
    queue_.push(std::move(r));
    return;
  }

  int sheetIdx = mz_zip_reader_locate_file(&zip, sheet_path.c_str(), nullptr, 0);
  if (sheetIdx < 0) {
    mz_zip_reader_end(&zip);
    XlsxBatchResult r;
    r.kind = XlsxResultKind::Error;
    r.error_message = "XLSX: sheet file not found in archive";
    queue_.push(std::move(r));
    return;
  }

  size_t sheetSize = 0;
  void* sheetBuf = mz_zip_reader_extract_to_heap(&zip, sheetIdx, &sheetSize, 0);
  mz_zip_reader_end(&zip);
  if (!sheetBuf) {
    XlsxBatchResult r;
    r.kind = XlsxResultKind::Error;
    r.error_message = "XLSX: failed to extract sheet XML";
    queue_.push(std::move(r));
    return;
  }

  const char* xml = static_cast<const char*>(sheetBuf);
  std::vector<std::string> headers;
  Batch batch;
  batch.reserve(options_.batch_size);
  bool first_row = true;

  auto on_row = [&](std::vector<std::string>&& row) {
    if (stop_requested_.load()) return false;
    if (first_row && options_.headers) {
      headers = std::move(row);
      first_row = false;
      return true;
    }
    if (first_row && !options_.headers && headers.empty()) {
      if (!options_.schema.empty()) {
        for (const auto& p : options_.schema) headers.push_back(p.first);
      } else {
        headers.reserve(row.size());
        for (size_t i = 0; i < row.size(); ++i)
          headers.push_back("Column" + std::to_string(i + 1));
      }
      first_row = false;
    }
    batch.push_back(std::move(row));
    if (batch.size() >= options_.batch_size) {
      XlsxBatch xb;
      xlsxBatchFromRows(
          std::vector<std::string>(headers),
          batch,
          options_,
          xb);
      XlsxBatchResult result;
      result.kind = XlsxResultKind::Batch;
      result.batch = std::move(xb);
      if (!queue_.push(std::move(result))) {
        mz_free(sheetBuf);
        return false;
      }
      batch.clear();
      batch.reserve(options_.batch_size);
    }
    return true;
  };

  xlsxParseSheetXml(xml, sheetSize, shared_strings, on_row);
  mz_free(sheetBuf);

  if (!batch.empty()) {
    XlsxBatch xb;
    xlsxBatchFromRows(
        std::vector<std::string>(headers),
        batch,
        options_,
        xb);
    XlsxBatchResult result;
    result.kind = XlsxResultKind::Batch;
    result.batch = std::move(xb);
    if (!queue_.push(std::move(result))) return;
  }

  {
    XlsxBatchResult result;
    result.kind = XlsxResultKind::Done;
    queue_.push(std::move(result));
  }
  } catch (const std::exception& e) {
    XlsxBatchResult r;
    r.kind = XlsxResultKind::Error;
    r.error_message = "XLSX parser error: ";
    r.error_message += e.what();
    queue_.push(std::move(r));
  } catch (...) {
    XlsxBatchResult r;
    r.kind = XlsxResultKind::Error;
    r.error_message = "XLSX parser error: unknown exception";
    queue_.push(std::move(r));
  }
}

}  // namespace ultratab
