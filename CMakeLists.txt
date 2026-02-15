cmake_minimum_required(VERSION 3.15...3.31)
project(ultratab)

# N-API configuration (NAPI_VERSION > 5 required for BigInt64Array)
add_compile_definitions(NAPI_VERSION=8)
add_compile_definitions(NAPI_DISABLE_CPP_EXCEPTIONS)

# C++17
set(CMAKE_CXX_STANDARD 17)
set(CMAKE_CXX_STANDARD_REQUIRED ON)
set(CMAKE_CXX_EXTENSIONS OFF)

# Source files
set(ULTRATAB_SOURCES
  src/addon.cc
  src/arena.cc
  src/batch_builder.cc
  src/columnar_parser.cc
  src/csv_parser.cc
  src/reader.cc
  src/simd_scanner.cc
  src/slice_parser.cc
  src/streaming_columnar_parser.cc
  src/streaming_parser.cc
  src/streaming_xlsx_parser.cc
  src/xlsx_parser.cc
  vendor/miniz.c
  vendor/miniz_tdef.c
  vendor/miniz_tinfl.c
  vendor/miniz_zip.c
)

add_library(${PROJECT_NAME} SHARED ${ULTRATAB_SOURCES} ${CMAKE_JS_SRC})
set_target_properties(${PROJECT_NAME} PROPERTIES PREFIX "" SUFFIX ".node")
target_include_directories(${PROJECT_NAME} PRIVATE ${CMAKE_JS_INC} ${CMAKE_CURRENT_SOURCE_DIR}/vendor)
target_link_libraries(${PROJECT_NAME} PRIVATE ${CMAKE_JS_LIB})

# Enable compiler warnings
if(MSVC)
  target_compile_options(${PROJECT_NAME} PRIVATE /W3 /WX-)
  # Enable C++ exceptions (required for node-addon-api patterns)
  target_compile_options(${PROJECT_NAME} PRIVATE /EHsc)
else()
  target_compile_options(${PROJECT_NAME} PRIVATE
    -Wall
    -Wextra
    -Wpedantic
  )
  # Enable C++ exceptions (node-gyp removes -fno-exceptions; ensure exceptions work)
  if(CMAKE_CXX_COMPILER_ID MATCHES "GNU|Clang")
    target_compile_options(${PROJECT_NAME} PRIVATE -fexceptions)
  endif()
endif()

# Platform-specific definitions
if(UNIX AND NOT APPLE)
  target_compile_definitions(${PROJECT_NAME} PRIVATE _LARGEFILE64_SOURCE)
endif()

# Conditional SIMD flags for x86_64 (AVX2 when supported)
# macOS: scalar only (toolchain header conflict with emmintrin.h per README)
# Linux/Windows x86_64: enable AVX2/SSE2
if(MSVC)
  if(CMAKE_SIZEOF_VOID_P EQUAL 8)
    target_compile_options(${PROJECT_NAME} PRIVATE /arch:AVX2)
  endif()
elseif(UNIX AND NOT APPLE)
  # Linux x86_64
  if(CMAKE_SYSTEM_PROCESSOR MATCHES "x86_64|amd64|AMD64" OR CMAKE_HOST_SYSTEM_PROCESSOR MATCHES "x86_64|amd64|AMD64")
    target_compile_options(${PROJECT_NAME} PRIVATE -msse2 -mavx2)
  endif()
endif()
# macOS: no SIMD flags (scalar path only)

# Windows: generate node.lib for Node-API (required by cmake-js)
if(MSVC AND CMAKE_JS_NODELIB_DEF AND CMAKE_JS_NODELIB_TARGET)
  execute_process(
    COMMAND ${CMAKE_AR} /def:${CMAKE_JS_NODELIB_DEF} /out:${CMAKE_JS_NODELIB_TARGET} ${CMAKE_STATIC_LINKER_FLAGS}
  )
endif()
