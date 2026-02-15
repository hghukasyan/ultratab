#ifndef ULTRATAB_ARENA_H
#define ULTRATAB_ARENA_H

#include <cstddef>
#include <cstdint>
#include <vector>

namespace ultratab {

struct PipelineMetrics;

/// Production-grade arena allocator: large blocks, bump-pointer, reset per batch.
/// Used for temporary parse structures, slice byte storage, and row metadata.
/// Does not hold memory that must survive beyond batch emission; copy out on takeBatch.
class Arena {
 public:
  /// \a block_size bytes per block (clamped to [1<<20, 16<<20]).
  explicit Arena(std::size_t block_size = 1024 * 1024);

  ~Arena();

  Arena(const Arena&) = delete;
  Arena& operator=(const Arena&) = delete;
  Arena(Arena&&) = default;
  Arena& operator=(Arena&&) = default;

  /// Optional: when set, arena updates these atomics on allocate/reset.
  void setMetrics(PipelineMetrics* m) { metrics_ = m; }

  /// Allocate \a size bytes with optional \a alignment (power of two). Returns pointer to write
  /// and logical offset (for FieldSlice.offset) into the linearized buffer.
  /// Alignment applies to the returned pointer; logical offset may differ.
  void* allocate(std::size_t size, std::size_t alignment, std::size_t* out_logical_offset);

  /// Convenience: allocate and copy \a data[0..size]; returns logical offset.
  std::size_t write(const char* data, std::size_t size);

  /// Total bytes currently used (sum of used bytes in all blocks). Offsets are in [0, used()).
  std::size_t used() const { return logical_used_; }

  /// Copy all used bytes in order (block0[0..used0], block1[0..used1], ...) into \a out.
  void copyUsedTo(std::vector<char>& out) const;

  /// Reset bump pointers so all blocks can be reused. Does not free blocks; updates stats.
  void reset();

  /// Current total capacity (sum of all block sizes). Debug only.
  std::uint64_t bytesAllocated() const { return bytes_allocated_; }

  /// Number of blocks. Debug only.
  std::uint64_t blockCount() const { return static_cast<std::uint64_t>(blocks_.size()); }

  /// Number of reset() calls. Debug only.
  std::uint64_t resetCount() const { return resets_; }

  /// Maximum value of used() observed. Debug only.
  std::uint64_t peakUsage() const { return peak_usage_; }

 private:
  struct Block {
    char* data = nullptr;
    std::size_t capacity = 0;
    std::size_t used = 0;
  };

  void addBlock();
  void updatePeakUsage();

  std::size_t block_size_;
  std::vector<Block> blocks_;
  std::size_t logical_used_ = 0;
  std::uint64_t bytes_allocated_ = 0;
  std::uint64_t resets_ = 0;
  std::uint64_t peak_usage_ = 0;
  PipelineMetrics* metrics_ = nullptr;
};

}  // namespace ultratab

#endif  // ULTRATAB_ARENA_H
