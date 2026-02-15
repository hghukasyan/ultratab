#include "arena.h"
#include "pipeline_metrics.h"
#include <cstdlib>
#include <cstring>
#include <algorithm>

namespace ultratab {

namespace {

const std::size_t kMinBlockSize = 1024 * 1024;   // 1MB
const std::size_t kMaxBlockSize = 16 * 1024 * 1024;  // 16MB

inline std::size_t alignUp(std::size_t value, std::size_t alignment) {
  return (value + alignment - 1) & ~(alignment - 1);
}

}  // namespace

Arena::Arena(std::size_t block_size) {
  block_size_ = std::max(kMinBlockSize, std::min(kMaxBlockSize, block_size));
}

Arena::~Arena() {
  for (Block& b : blocks_) {
    std::free(b.data);
    b.data = nullptr;
    b.capacity = 0;
    b.used = 0;
  }
  blocks_.clear();
}

void Arena::addBlock() {
  Block b;
  b.data = static_cast<char*>(std::malloc(block_size_));
  if (!b.data) {
    std::abort();
  }
  b.capacity = block_size_;
  b.used = 0;
  blocks_.push_back(b);
  bytes_allocated_ += block_size_;
  if (metrics_) {
    metrics_->arena_bytes_allocated.store(bytes_allocated_);
    metrics_->arena_blocks.store(static_cast<std::uint64_t>(blocks_.size()));
  }
}

void Arena::updatePeakUsage() {
  if (logical_used_ > peak_usage_) {
    peak_usage_ = logical_used_;
    if (metrics_) {
      metrics_->peak_arena_usage.store(peak_usage_);
    }
  }
}

void* Arena::allocate(std::size_t size, std::size_t alignment,
                      std::size_t* out_logical_offset) {
  if (size == 0) {
    if (out_logical_offset) *out_logical_offset = logical_used_;
    return nullptr;
  }
  if (alignment == 0) alignment = 1;
  if ((alignment & (alignment - 1)) != 0) alignment = 1;

  if (blocks_.empty()) {
    addBlock();
  }

  Block& cur = blocks_.back();
  std::size_t aligned_used = alignUp(cur.used, alignment);
  std::size_t need = aligned_used + size;
  if (need > cur.capacity) {
    addBlock();
    Block& next = blocks_.back();
    std::size_t aligned_start = alignUp(0, alignment);
    if (out_logical_offset) *out_logical_offset = logical_used_;
    logical_used_ += size;
    next.used = aligned_start + size;
    updatePeakUsage();
    if (metrics_) {
      metrics_->arena_bytes_allocated.store(bytes_allocated_);
    }
    return next.data + aligned_start;
  }

  cur.used = aligned_used + size;
  if (out_logical_offset) *out_logical_offset = logical_used_;
  logical_used_ += size;
  updatePeakUsage();
  return cur.data + aligned_used;
}

std::size_t Arena::write(const char* data, std::size_t size) {
  std::size_t off;
  void* ptr = allocate(size, 1, &off);
  if (ptr && data && size > 0) {
    std::memcpy(ptr, data, size);
  }
  return off;
}

void Arena::copyUsedTo(std::vector<char>& out) const {
  out.clear();
  out.reserve(logical_used_);
  for (const Block& b : blocks_) {
    if (b.used > 0) {
      out.insert(out.end(), b.data, b.data + b.used);
    }
  }
}

void Arena::reset() {
  for (Block& b : blocks_) {
    b.used = 0;
  }
  logical_used_ = 0;
  ++resets_;
  if (metrics_) {
    metrics_->arena_resets.store(resets_);
  }
}

}  // namespace ultratab
