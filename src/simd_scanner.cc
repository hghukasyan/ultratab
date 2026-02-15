#include "simd_scanner.h"
#include <cstddef>
#include <cstdlib>
#include <cstring>

#if defined(__x86_64__) || defined(_M_X64) || defined(__i386__) || defined(_M_IX86)

#if defined(_MSC_VER)
#include <intrin.h>
#define ULTRATAB_CPUID(info, leaf, subleaf) \
  __cpuidex(reinterpret_cast<int*>(info), static_cast<int>(leaf), static_cast<int>(subleaf))
static inline unsigned ctz32(unsigned x) {
  unsigned long idx;
  return _BitScanForward(&idx, x) ? idx : 32;
}
#else
#include <cpuid.h>
#define ULTRATAB_CPUID(info, leaf, subleaf) \
  __cpuid_count(leaf, subleaf, (info)[0], (info)[1], (info)[2], (info)[3])
static inline unsigned ctz32(unsigned x) {
  return x ? static_cast<unsigned>(__builtin_ctz(x)) : 32u;
}
#endif

#else
#define ULTRATAB_NO_CPUID 1
static inline unsigned ctz32(unsigned) { return 32; }
#endif

namespace ultratab {

namespace {

#if !defined(ULTRATAB_NO_CPUID)

CpuFeatures detectCpuFeaturesImpl() {
  CpuFeatures f;
#if defined(__x86_64__) || defined(_M_X64) || defined(__i386__) || defined(_M_IX86)
  int info[4];
  ULTRATAB_CPUID(info, 1, 0);
  f.sse2 = (static_cast<unsigned>(info[3]) & (1u << 26)) != 0;

  ULTRATAB_CPUID(info, 7, 0);
  f.avx2 = (static_cast<unsigned>(info[1]) & (1u << 5)) != 0;
#endif
  return f;
}

#endif  // !ULTRATAB_NO_CPUID

}  // namespace

CpuFeatures detectCpuFeatures() {
#if defined(ULTRATAB_NO_CPUID)
  return CpuFeatures{};
#else
  static CpuFeatures cached = detectCpuFeaturesImpl();
  return cached;
#endif
}

// --- Scalar fallback ---

static std::size_t scanForSeparatorScalar(const char* data, std::size_t len,
                                          char delimiter) {
  for (std::size_t i = 0; i < len; ++i) {
    char c = data[i];
    if (c == delimiter || c == '\r' || c == '\n') return i;
  }
  return len;
}

static std::size_t scanForNewlineScalar(const char* data, std::size_t len) {
  for (std::size_t i = 0; i < len; ++i) {
    char c = data[i];
    if (c == '\r' || c == '\n') return i;
  }
  return len;
}

static std::size_t scanForCharScalar(const char* data, std::size_t len, char ch) {
  for (std::size_t i = 0; i < len; ++i) {
    if (data[i] == ch) return i;
  }
  return len;
}

// --- SSE2 path (16 bytes at a time) ---

#if defined(__SSE2__) || (defined(_MSC_VER) && (defined(_M_X64) || _M_IX86_FP >= 2))

#include <emmintrin.h>

static std::size_t scanForSeparatorSSE2(const char* data, std::size_t len,
                                        char delimiter) {
  __m128i delim_v = _mm_set1_epi8(static_cast<char>(delimiter));
  __m128i cr_v = _mm_set1_epi8('\r');
  __m128i lf_v = _mm_set1_epi8('\n');

  const char* p = data;
  const char* end = data + len;

  while (p + 16 <= end) {
    __m128i chunk = _mm_loadu_si128(reinterpret_cast<const __m128i*>(p));
    __m128i eq_delim = _mm_cmpeq_epi8(chunk, delim_v);
    __m128i eq_cr = _mm_cmpeq_epi8(chunk, cr_v);
    __m128i eq_lf = _mm_cmpeq_epi8(chunk, lf_v);
    __m128i any = _mm_or_si128(_mm_or_si128(eq_delim, eq_cr), eq_lf);
    int mask = _mm_movemask_epi8(any);
    if (mask != 0) {
      unsigned idx = ctz32(static_cast<unsigned>(mask));
      return static_cast<std::size_t>(p - data + idx);
    }
    p += 16;
  }

  return static_cast<std::size_t>(p - data) +
         scanForSeparatorScalar(p, static_cast<std::size_t>(end - p), delimiter);
}

static std::size_t scanForNewlineSSE2(const char* data, std::size_t len) {
  __m128i cr_v = _mm_set1_epi8('\r');
  __m128i lf_v = _mm_set1_epi8('\n');

  const char* p = data;
  const char* end = data + len;

  while (p + 16 <= end) {
    __m128i chunk = _mm_loadu_si128(reinterpret_cast<const __m128i*>(p));
    __m128i eq_cr = _mm_cmpeq_epi8(chunk, cr_v);
    __m128i eq_lf = _mm_cmpeq_epi8(chunk, lf_v);
    __m128i any = _mm_or_si128(eq_cr, eq_lf);
    int mask = _mm_movemask_epi8(any);
    if (mask != 0) {
      unsigned idx = ctz32(static_cast<unsigned>(mask));
      return static_cast<std::size_t>(p - data + idx);
    }
    p += 16;
  }

  return static_cast<std::size_t>(p - data) +
         scanForNewlineScalar(p, static_cast<std::size_t>(end - p));
}

static std::size_t scanForCharSSE2(const char* data, std::size_t len, char ch) {
  __m128i ch_v = _mm_set1_epi8(static_cast<char>(ch));
  const char* p = data;
  const char* end = data + len;
  while (p + 16 <= end) {
    __m128i chunk = _mm_loadu_si128(reinterpret_cast<const __m128i*>(p));
    __m128i eq = _mm_cmpeq_epi8(chunk, ch_v);
    int mask = _mm_movemask_epi8(eq);
    if (mask != 0) {
      unsigned idx = ctz32(static_cast<unsigned>(mask));
      return static_cast<std::size_t>(p - data + idx);
    }
    p += 16;
  }
  return static_cast<std::size_t>(p - data) +
         scanForCharScalar(p, static_cast<std::size_t>(end - p), ch);
}

#endif  // SSE2

// --- AVX2 path (32 bytes at a time) ---

#if defined(__AVX2__) || (defined(_MSC_VER) && defined(__AVX2__))

#include <immintrin.h>

static std::size_t scanForSeparatorAVX2(const char* data, std::size_t len,
                                        char delimiter) {
  __m256i delim_v = _mm256_set1_epi8(static_cast<char>(delimiter));
  __m256i cr_v = _mm256_set1_epi8('\r');
  __m256i lf_v = _mm256_set1_epi8('\n');

  const char* p = data;
  const char* end = data + len;

  while (p + 32 <= end) {
    __m256i chunk = _mm256_loadu_si256(reinterpret_cast<const __m256i*>(p));
    __m256i eq_delim = _mm256_cmpeq_epi8(chunk, delim_v);
    __m256i eq_cr = _mm256_cmpeq_epi8(chunk, cr_v);
    __m256i eq_lf = _mm256_cmpeq_epi8(chunk, lf_v);
    __m256i any = _mm256_or_si256(
        _mm256_or_si256(eq_delim, eq_cr), eq_lf);
    int mask = _mm256_movemask_epi8(any);
    if (mask != 0) {
      unsigned idx = ctz32(static_cast<unsigned>(mask));
      return static_cast<std::size_t>(p - data + idx);
    }
    p += 32;
  }

  return static_cast<std::size_t>(p - data) +
         scanForSeparatorScalar(p, static_cast<std::size_t>(end - p), delimiter);
}

static std::size_t scanForNewlineAVX2(const char* data, std::size_t len) {
  __m256i cr_v = _mm256_set1_epi8('\r');
  __m256i lf_v = _mm256_set1_epi8('\n');

  const char* p = data;
  const char* end = data + len;

  while (p + 32 <= end) {
    __m256i chunk = _mm256_loadu_si256(reinterpret_cast<const __m256i*>(p));
    __m256i eq_cr = _mm256_cmpeq_epi8(chunk, cr_v);
    __m256i eq_lf = _mm256_cmpeq_epi8(chunk, lf_v);
    __m256i any = _mm256_or_si256(eq_cr, eq_lf);
    int mask = _mm256_movemask_epi8(any);
    if (mask != 0) {
      unsigned idx = ctz32(static_cast<unsigned>(mask));
      return static_cast<std::size_t>(p - data + idx);
    }
    p += 32;
  }

  return static_cast<std::size_t>(p - data) +
         scanForNewlineScalar(p, static_cast<std::size_t>(end - p));
}

static std::size_t scanForCharAVX2(const char* data, std::size_t len, char ch) {
  __m256i ch_v = _mm256_set1_epi8(static_cast<char>(ch));
  const char* p = data;
  const char* end = data + len;
  while (p + 32 <= end) {
    __m256i chunk = _mm256_loadu_si256(reinterpret_cast<const __m256i*>(p));
    __m256i eq = _mm256_cmpeq_epi8(chunk, ch_v);
    int mask = _mm256_movemask_epi8(eq);
    if (mask != 0) {
      unsigned idx = ctz32(static_cast<unsigned>(mask));
      return static_cast<std::size_t>(p - data + idx);
    }
    p += 32;
  }
  return static_cast<std::size_t>(p - data) +
         scanForCharScalar(p, static_cast<std::size_t>(end - p), ch);
}

#endif  // AVX2

std::size_t scanForSeparator(const char* data, std::size_t len, char delimiter,
                             const CpuFeatures& features) {
#if defined(__AVX2__)
  if (features.avx2) return scanForSeparatorAVX2(data, len, delimiter);
#endif
#if defined(__SSE2__) || (defined(_MSC_VER) && (defined(_M_X64) || _M_IX86_FP >= 2))
  if (features.sse2) return scanForSeparatorSSE2(data, len, delimiter);
#endif
  return scanForSeparatorScalar(data, len, delimiter);
}

std::size_t scanForNewline(const char* data, std::size_t len,
                           const CpuFeatures& features) {
#if defined(__AVX2__)
  if (features.avx2) return scanForNewlineAVX2(data, len);
#endif
#if defined(__SSE2__) || (defined(_MSC_VER) && (defined(_M_X64) || _M_IX86_FP >= 2))
  if (features.sse2) return scanForNewlineSSE2(data, len);
#endif
  return scanForNewlineScalar(data, len);
}

std::size_t scanForChar(const char* data, std::size_t len, char ch,
                        const CpuFeatures& features) {
#if defined(__AVX2__)
  if (features.avx2) return scanForCharAVX2(data, len, ch);
#endif
#if defined(__SSE2__) || (defined(_MSC_VER) && (defined(_M_X64) || _M_IX86_FP >= 2))
  if (features.sse2) return scanForCharSSE2(data, len, ch);
#endif
  return scanForCharScalar(data, len, ch);
}

}  // namespace ultratab
