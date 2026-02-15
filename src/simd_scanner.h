#ifndef ULTRATAB_SIMD_SCANNER_H
#define ULTRATAB_SIMD_SCANNER_H

#include <cstddef>

namespace ultratab {

/// CPU feature flags (runtime detection).
struct CpuFeatures {
  bool sse2 = false;
  bool avx2 = false;
};

/// Detect CPU features at runtime. Thread-safe.
CpuFeatures detectCpuFeatures();

/// Fast scanner: find next delimiter or newline in data.
/// Used to accelerate scanning when NOT inside quotes; state machine validates.
/// Returns offset from start, or len if not found.
std::size_t scanForSeparator(const char* data, std::size_t len, char delimiter,
                             const CpuFeatures& features);

/// Scan for newline only (CR or LF). Returns offset or len.
std::size_t scanForNewline(const char* data, std::size_t len,
                           const CpuFeatures& features);

/// Scan for a single character. Returns offset or len.
std::size_t scanForChar(const char* data, std::size_t len, char ch,
                        const CpuFeatures& features);

}  // namespace ultratab

#endif  // ULTRATAB_SIMD_SCANNER_H
