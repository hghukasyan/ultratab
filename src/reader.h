#ifndef ULTRATAB_READER_H
#define ULTRATAB_READER_H

#include <cstddef>
#include <string>
#include <utility>
#include <vector>

namespace ultratab {

/// Non-owning byte span for zero-copy handoff.
struct ByteSpan {
  const char* data = nullptr;
  std::size_t size = 0;
  bool empty() const { return size == 0; }
};

/// Reader options: buffered vs mmap, buffer size.
struct ReaderOptions {
  bool use_mmap = false;
  std::size_t buffer_size = 256 * 1024;  // 256 KB default for buffered
};

/// File reader stage: produces byte chunks from disk.
/// Buffered: large reads into internal buffer; mmap: whole file (or segment) as one span.
class FileReader {
 public:
  explicit FileReader(const std::string& path, const ReaderOptions& options);
  ~FileReader();

  FileReader(const FileReader&) = delete;
  FileReader& operator=(const FileReader&) = delete;

  /// Next chunk. Buffered: (ptr, len) into internal buffer; valid until next getNext().
  /// Mmap: single span for whole file; getNext() returns it once then (nullptr, 0).
  /// Returns (nullptr, 0) on EOF or error.
  ByteSpan getNext();

  /// Total bytes read so far (for metrics). Buffered: sum of chunk sizes; mmap: file size after first getNext().
  std::size_t bytesRead() const { return bytes_read_; }

  /// True if open failed (getNext will return empty).
  bool hasError() const { return error_; }
  const std::string& errorMessage() const { return error_message_; }

 private:
  bool openAndPrepare();
  ByteSpan getNextBuffered();
  ByteSpan getNextMmap();

  std::string path_;
  ReaderOptions options_;
  std::size_t bytes_read_ = 0;
  bool error_ = false;
  std::string error_message_;

  // Buffered mode
  std::vector<char> buffer_;
  int fd_ = -1;
#ifdef _WIN32
  void* handle_ = nullptr;  // HANDLE
#endif

  // Mmap mode: return whole file as one span
  const char* mmap_base_ = nullptr;
  std::size_t mmap_len_ = 0;
  bool mmap_returned_ = false;
#ifdef _WIN32
  void* file_handle_ = nullptr;
  void* map_handle_ = nullptr;
#endif
};

}  // namespace ultratab

#endif  // ULTRATAB_READER_H
