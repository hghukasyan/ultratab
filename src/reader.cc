#include "reader.h"
#include <cerrno>
#include <cstring>

#ifdef _WIN32
#ifndef WIN32_LEAN_AND_MEAN
#define WIN32_LEAN_AND_MEAN
#endif
#include <windows.h>
#include <fcntl.h>
#include <io.h>
#include <sys/stat.h>
#else
#include <fcntl.h>
#include <sys/mman.h>
#include <sys/stat.h>
#include <unistd.h>
#endif

namespace ultratab {

FileReader::FileReader(const std::string& path, const ReaderOptions& options)
    : path_(path), options_(options) {
  if (options_.use_mmap) {
    buffer_.clear();
    mmap_len_ = 0;
#ifdef _WIN32
    file_handle_ = reinterpret_cast<void*>(CreateFileA(
        path_.c_str(), GENERIC_READ, FILE_SHARE_READ, nullptr, OPEN_EXISTING,
        FILE_ATTRIBUTE_NORMAL, nullptr));
    if (file_handle_ == nullptr || file_handle_ == INVALID_HANDLE_VALUE) {
      error_ = true;
      error_message_ = "Failed to open file: ";
      error_message_ += path_;
      return;
    }
    LARGE_INTEGER li;
    if (!GetFileSizeEx(reinterpret_cast<HANDLE>(file_handle_), &li) ||
        li.QuadPart == 0) {
      CloseHandle(reinterpret_cast<HANDLE>(file_handle_));
      file_handle_ = nullptr;
      error_ = (li.QuadPart != 0);
      if (li.QuadPart == 0) {
        mmap_len_ = 0;
        mmap_base_ = nullptr;
        mmap_returned_ = true;
      }
      return;
    }
    mmap_len_ = static_cast<std::size_t>(li.QuadPart);
    map_handle_ = CreateFileMappingA(reinterpret_cast<HANDLE>(file_handle_),
                                    nullptr, PAGE_READONLY, 0, 0, nullptr);
    if (!map_handle_) {
      CloseHandle(reinterpret_cast<HANDLE>(file_handle_));
      file_handle_ = nullptr;
      error_ = true;
      error_message_ = "CreateFileMapping failed";
      return;
    }
    mmap_base_ = static_cast<const char*>(MapViewOfFile(
        reinterpret_cast<HANDLE>(map_handle_), FILE_MAP_READ, 0, 0, 0));
    if (!mmap_base_) {
      CloseHandle(reinterpret_cast<HANDLE>(map_handle_));
      CloseHandle(reinterpret_cast<HANDLE>(file_handle_));
      map_handle_ = nullptr;
      file_handle_ = nullptr;
      error_ = true;
      error_message_ = "MapViewOfFile failed";
      return;
    }
    bytes_read_ = mmap_len_;
#else
    fd_ = ::open(path_.c_str(), O_RDONLY);
    if (fd_ < 0) {
      error_ = true;
      error_message_ = std::string("Failed to open file: ") + path_ + " " + std::strerror(errno);
      return;
    }
    struct stat st;
    if (fstat(fd_, &st) != 0 || st.st_size <= 0) {
      if (st.st_size == 0) {
        mmap_len_ = 0;
        mmap_base_ = nullptr;
        mmap_returned_ = true;
        return;
      }
      error_ = true;
      error_message_ = std::strerror(errno);
      ::close(fd_);
      fd_ = -1;
      return;
    }
    mmap_len_ = static_cast<std::size_t>(st.st_size);
    void* p = mmap(nullptr, mmap_len_, PROT_READ, MAP_PRIVATE, fd_, 0);
    if (p == MAP_FAILED) {
      error_message_ = std::strerror(errno);
      ::close(fd_);
      fd_ = -1;
      error_ = true;
      return;
    }
    mmap_base_ = static_cast<const char*>(p);
    bytes_read_ = mmap_len_;
#endif
    return;
  }

  // Buffered mode
  if (!openAndPrepare()) return;
  buffer_.resize(options_.buffer_size > 0 ? options_.buffer_size : 256 * 1024);
}

FileReader::~FileReader() {
  if (options_.use_mmap) {
#ifdef _WIN32
    if (mmap_base_) {
      UnmapViewOfFile(mmap_base_);
      mmap_base_ = nullptr;
    }
    if (map_handle_) {
      CloseHandle(reinterpret_cast<HANDLE>(map_handle_));
      map_handle_ = nullptr;
    }
    if (file_handle_) {
      CloseHandle(reinterpret_cast<HANDLE>(file_handle_));
      file_handle_ = nullptr;
    }
#else
    if (mmap_base_ && mmap_len_ > 0) {
      munmap(const_cast<char*>(mmap_base_), mmap_len_);
      mmap_base_ = nullptr;
      mmap_len_ = 0;
    }
    if (fd_ >= 0) {
      ::close(fd_);
      fd_ = -1;
    }
#endif
    return;
  }
#ifdef _WIN32
  if (handle_ != nullptr && handle_ != INVALID_HANDLE_VALUE) {
    _close(reinterpret_cast<int>(reinterpret_cast<intptr_t>(handle_)));
    handle_ = nullptr;
  }
#else
  if (fd_ >= 0) {
    ::close(fd_);
    fd_ = -1;
  }
#endif
}

bool FileReader::openAndPrepare() {
  if (options_.use_mmap) return true;
#ifdef _WIN32
  fd_ = _open(path_.c_str(), _O_RDONLY | _O_BINARY);
  if (fd_ < 0) {
    error_ = true;
    error_message_ = std::string("Failed to open: ") + path_;
    return false;
  }
  handle_ = reinterpret_cast<void*>(static_cast<intptr_t>(fd_));
#else
  fd_ = ::open(path_.c_str(), O_RDONLY);
  if (fd_ < 0) {
    error_ = true;
    error_message_ = std::string("Failed to open: ") + path_ + " " + std::strerror(errno);
    return false;
  }
#endif
  return true;
}

ByteSpan FileReader::getNext() {
  if (error_) return {nullptr, 0};
  if (options_.use_mmap) return getNextMmap();
  return getNextBuffered();
}

ByteSpan FileReader::getNextBuffered() {
#ifdef _WIN32
  int n = _read(fd_, buffer_.data(), static_cast<unsigned>(buffer_.size()));
#else
  ssize_t n = ::read(fd_, buffer_.data(), buffer_.size());
#endif
  if (n <= 0) return {nullptr, 0};
  std::size_t u = static_cast<std::size_t>(n);
  bytes_read_ += u;
  return {buffer_.data(), u};
}

ByteSpan FileReader::getNextMmap() {
  if (mmap_returned_) return {nullptr, 0};
  mmap_returned_ = true;
  if (!mmap_base_ || mmap_len_ == 0) return {nullptr, 0};
  return {mmap_base_, mmap_len_};
}

}  // namespace ultratab
