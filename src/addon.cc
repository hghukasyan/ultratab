#include "streaming_parser.h"
#include "streaming_columnar_parser.h"
#include "streaming_xlsx_parser.h"
#include "pipeline_metrics.h"
#include <napi.h>
#include <cstring>
#include <memory>

namespace ultratab {

using namespace Napi;

static Value BatchToValue(Env env, const Batch& batch) {
  Array arr = Array::New(env, batch.size());
  for (std::size_t i = 0; i < batch.size(); ++i) {
    Array row = Array::New(env, batch[i].size());
    for (std::size_t j = 0; j < batch[i].size(); ++j) {
      row[j] = String::New(env, batch[i][j]);
    }
    arr[i] = row;
  }
  return arr;
}

class GetNextBatchWorker : public AsyncWorker {
 public:
  GetNextBatchWorker(Napi::Env env, StreamingCsvParser* parser)
      : AsyncWorker(env),
        deferred_(Promise::Deferred::New(env)),
        parser_(parser),
        result_kind_(BatchResultKind::Done) {}

  Promise GetPromise() { return deferred_.Promise(); }

  void Execute() override {
    BatchResult result;
    if (!parser_->queue().pop(result)) {
      result_kind_ = BatchResultKind::Cancelled;
      return;
    }
    result_kind_ = result.kind;
    if (result.kind == BatchResultKind::Error) {
      SetError(result.error_message);
      return;
    }
    if (result.kind == BatchResultKind::Batch) {
      batch_ = std::move(result.batch);
    }
  }

  void OnOK() override {
    if (result_kind_ == BatchResultKind::Done ||
        result_kind_ == BatchResultKind::Cancelled) {
      deferred_.Resolve(Env().Undefined());
      return;
    }
    deferred_.Resolve(BatchToValue(Env(), batch_));
  }

  void OnError(const Error& e) override { deferred_.Reject(e.Value()); }

 private:
  Promise::Deferred deferred_;
  StreamingCsvParser* parser_;
  BatchResultKind result_kind_;
  Batch batch_;
};

static Value CreateParser(const CallbackInfo& info) {
  Env env = info.Env();
  if (info.Length() < 1 || !info[0].IsString()) {
    TypeError::New(env, "Expected path (string) as first argument")
        .ThrowAsJavaScriptException();
    return env.Null();
  }
  std::string path = info[0].As<String>().Utf8Value();

  CsvOptions opts;
  if (info.Length() >= 2 && info[1].IsObject()) {
    Object options = info[1].As<Object>();
    if (options.Has("delimiter")) {
      Value d = options.Get("delimiter");
      if (d.IsString()) {
        std::string s = d.As<String>().Utf8Value();
        if (!s.empty()) opts.delimiter = s[0];
      }
    }
    if (options.Has("quote")) {
      Value q = options.Get("quote");
      if (q.IsString()) {
        std::string s = q.As<String>().Utf8Value();
        if (!s.empty()) opts.quote = s[0];
      }
    }
    if (options.Has("headers")) {
      Value h = options.Get("headers");
      if (h.IsBoolean()) opts.has_header = h.As<Boolean>().Value();
    }
    if (options.Has("batchSize")) {
      Value b = options.Get("batchSize");
      if (b.IsNumber()) {
        double n = b.As<Number>().DoubleValue();
        if (n >= 1 && n <= 10000000)
          opts.batch_size = static_cast<std::size_t>(n);
      }
    }
  }

  std::size_t max_queue = 2;
  bool use_mmap = false;
  std::size_t read_buffer_size = 0;
  if (info.Length() >= 2 && info[1].IsObject()) {
    Object options = info[1].As<Object>();
    if (options.Has("maxQueueBatches")) {
      Value v = options.Get("maxQueueBatches");
      if (v.IsNumber()) {
        double n = v.As<Number>().DoubleValue();
        if (n >= 1 && n <= 256) max_queue = static_cast<std::size_t>(n);
      }
    }
    if (options.Has("useMmap")) {
      Value v = options.Get("useMmap");
      if (v.IsBoolean()) use_mmap = v.As<Boolean>().Value();
    }
    if (options.Has("readBufferSize")) {
      Value v = options.Get("readBufferSize");
      if (v.IsNumber()) {
        double n = v.As<Number>().DoubleValue();
        if (n >= 4096 && n <= 64 * 1024 * 1024)
          read_buffer_size = static_cast<std::size_t>(n);
      }
    }
  }

  try {
    auto* parser = new StreamingCsvParser(path, opts, max_queue, use_mmap,
                                         read_buffer_size);
    return External<StreamingCsvParser>::New(env, parser);
  } catch (const std::exception& e) {
    Error::New(env, std::string("Failed to create parser: ") + e.what())
        .ThrowAsJavaScriptException();
    return env.Null();
  }
}

static Value GetNextBatch(const CallbackInfo& info) {
  Env env = info.Env();
  if (info.Length() < 1 || !info[0].IsExternal()) {
    TypeError::New(env, "Expected parser (external) as first argument")
        .ThrowAsJavaScriptException();
    return env.Null();
  }
  auto* parser = info[0].As<External<StreamingCsvParser>>().Data();
  auto* worker = new GetNextBatchWorker(env, parser);
  worker->Queue();
  return worker->GetPromise();
}

static Value DestroyParser(const CallbackInfo& info) {
  Env env = info.Env();
  if (info.Length() < 1 || !info[0].IsExternal()) {
    TypeError::New(env, "Expected parser (external) as first argument")
        .ThrowAsJavaScriptException();
    return env.Undefined();
  }
  auto* parser = info[0].As<External<StreamingCsvParser>>().Data();
  parser->stop();
  delete parser;
  return env.Undefined();
}

static Value GetParserMetrics(const CallbackInfo& info) {
  Env env = info.Env();
  if (info.Length() < 1 || !info[0].IsExternal()) {
    TypeError::New(env, "Expected parser (external) as first argument")
        .ThrowAsJavaScriptException();
    return env.Null();
  }
  auto* parser = info[0].As<External<StreamingCsvParser>>().Data();
  const PipelineMetrics& m = parser->metrics();
  Object obj = Object::New(env);
  obj.Set("bytes_read", Number::New(env, static_cast<double>(m.bytes_read.load())));
  obj.Set("rows_parsed", Number::New(env, static_cast<double>(m.rows_parsed.load())));
  obj.Set("batches_emitted", Number::New(env, static_cast<double>(m.batches_emitted.load())));
  obj.Set("queue_wait_ns", Number::New(env, static_cast<double>(m.queue_wait_ns.load())));
  obj.Set("parse_time_ns", Number::New(env, static_cast<double>(m.parse_time_ns.load())));
  obj.Set("read_time_ns", Number::New(env, static_cast<double>(m.read_time_ns.load())));
  obj.Set("build_time_ns", Number::New(env, static_cast<double>(m.build_time_ns.load())));
  obj.Set("emit_time_ns", Number::New(env, static_cast<double>(m.emit_time_ns.load())));
  obj.Set("arena_resizes", Number::New(env, static_cast<double>(m.arena_resizes.load())));
  obj.Set("batch_allocations", Number::New(env, static_cast<double>(m.batch_allocations.load())));
  obj.Set("arena_bytes_allocated", Number::New(env, static_cast<double>(m.arena_bytes_allocated.load())));
  obj.Set("arena_blocks", Number::New(env, static_cast<double>(m.arena_blocks.load())));
  obj.Set("arena_resets", Number::New(env, static_cast<double>(m.arena_resets.load())));
  obj.Set("peak_arena_usage", Number::New(env, static_cast<double>(m.peak_arena_usage.load())));
  return obj;
}

// --- Columnar API ---

static Value ColumnarBatchToValue(Env env, const ColumnarBatch& batch) {
  Object obj = Object::New(env);
  Array headers = Array::New(env, batch.headers.size());
  for (std::size_t i = 0; i < batch.headers.size(); ++i) {
    headers[i] = String::New(env, batch.headers[i]);
  }
  obj.Set("headers", headers);
  obj.Set("rows", Number::New(env, static_cast<double>(batch.rows)));

  Object columns = Object::New(env);
  Object nullMask = Object::New(env);

  for (const auto& pair : batch.columns) {
    const std::string& name = pair.first;
    const ColumnarColumn& col = pair.second;

    switch (col.type) {
      case ColumnType::String: {
        Array arr = Array::New(env, col.strings.size());
        for (std::size_t i = 0; i < col.strings.size(); ++i) {
          arr[i] = String::New(env, col.strings[i]);
        }
        columns.Set(name, arr);
        break;
      }
      case ColumnType::Int32: {
        Int32Array arr = Int32Array::New(env, col.int32_data->size());
        std::memcpy(arr.Data(), col.int32_data->data(),
                    col.int32_data->size() * sizeof(std::int32_t));
        columns.Set(name, arr);
        if (col.null_mask) {
          Uint8Array nm = Uint8Array::New(env, col.null_mask->size());
          std::memcpy(nm.Data(), col.null_mask->data(), col.null_mask->size());
          nullMask.Set(name, nm);
        }
        break;
      }
      case ColumnType::Int64: {
        BigInt64Array arr = BigInt64Array::New(env, col.int64_data->size());
        std::memcpy(arr.Data(), col.int64_data->data(),
                    col.int64_data->size() * sizeof(std::int64_t));
        columns.Set(name, arr);
        if (col.null_mask) {
          Uint8Array nm = Uint8Array::New(env, col.null_mask->size());
          std::memcpy(nm.Data(), col.null_mask->data(), col.null_mask->size());
          nullMask.Set(name, nm);
        }
        break;
      }
      case ColumnType::Float64: {
        Float64Array arr = Float64Array::New(env, col.float64_data->size());
        std::memcpy(arr.Data(), col.float64_data->data(),
                    col.float64_data->size() * sizeof(double));
        columns.Set(name, arr);
        if (col.null_mask) {
          Uint8Array nm = Uint8Array::New(env, col.null_mask->size());
          std::memcpy(nm.Data(), col.null_mask->data(), col.null_mask->size());
          nullMask.Set(name, nm);
        }
        break;
      }
      case ColumnType::Bool: {
        Uint8Array arr = Uint8Array::New(env, col.bool_data->size());
        std::memcpy(arr.Data(), col.bool_data->data(), col.bool_data->size());
        columns.Set(name, arr);
        if (col.null_mask) {
          Uint8Array nm = Uint8Array::New(env, col.null_mask->size());
          std::memcpy(nm.Data(), col.null_mask->data(), col.null_mask->size());
          nullMask.Set(name, nm);
        }
        break;
      }
    }
  }
  obj.Set("columns", columns);

  bool hasNullMask = false;
  for (const auto& pair : batch.columns) {
    if (pair.second.null_mask && !pair.second.null_mask->empty()) {
      hasNullMask = true;
      break;
    }
  }
  if (hasNullMask) obj.Set("nullMask", nullMask);

  return obj;
}

static void ParseColumnarOptions(Env env, Object options, ColumnarOptions& opts) {
  if (options.Has("delimiter")) {
    Value d = options.Get("delimiter");
    if (d.IsString()) {
      std::string s = d.As<String>().Utf8Value();
      if (!s.empty()) opts.delimiter = s[0];
    }
  }
  if (options.Has("quote")) {
    Value q = options.Get("quote");
    if (q.IsString()) {
      std::string s = q.As<String>().Utf8Value();
      if (!s.empty()) opts.quote = s[0];
    }
  }
  if (options.Has("headers")) {
    Value h = options.Get("headers");
    if (h.IsBoolean()) opts.has_header = h.As<Boolean>().Value();
  }
  if (options.Has("batchSize")) {
    Value b = options.Get("batchSize");
    if (b.IsNumber()) {
      double n = b.As<Number>().DoubleValue();
      if (n >= 1 && n <= 10000000)
        opts.batch_size = static_cast<std::size_t>(n);
    }
  }
  if (options.Has("select")) {
    Value sel = options.Get("select");
    if (sel.IsArray()) {
      Array arr = sel.As<Array>();
      opts.select.clear();
      for (uint32_t i = 0; i < arr.Length(); ++i) {
        Value v = arr[i];
        if (v.IsString()) opts.select.push_back(v.As<String>().Utf8Value());
      }
    }
  }
  if (options.Has("schema")) {
    Value sch = options.Get("schema");
    if (sch.IsObject()) {
      Object schemaObj = sch.As<Object>();
      Array keys = schemaObj.GetPropertyNames();
      for (uint32_t i = 0; i < keys.Length(); ++i) {
        std::string key = keys.Get(i).As<String>().Utf8Value();
        Value v = schemaObj.Get(key);
        if (v.IsString()) {
          std::string t = v.As<String>().Utf8Value();
          if (t == "string") opts.schema[key] = ColumnType::String;
          else if (t == "int32") opts.schema[key] = ColumnType::Int32;
          else if (t == "int64") opts.schema[key] = ColumnType::Int64;
          else if (t == "float64") opts.schema[key] = ColumnType::Float64;
          else if (t == "bool") opts.schema[key] = ColumnType::Bool;
        }
      }
    }
  }
  if (options.Has("nullValues")) {
    Value nv = options.Get("nullValues");
    if (nv.IsArray()) {
      Array arr = nv.As<Array>();
      opts.null_values.clear();
      for (uint32_t i = 0; i < arr.Length(); ++i) {
        Value v = arr[i];
        if (v.IsString()) opts.null_values.push_back(v.As<String>().Utf8Value());
      }
    }
  }
  if (options.Has("trim")) {
    Value t = options.Get("trim");
    if (t.IsBoolean()) opts.trim = t.As<Boolean>().Value();
  }
  if (options.Has("typedFallback")) {
    Value tf = options.Get("typedFallback");
    if (tf.IsString()) {
      std::string s = tf.As<String>().Utf8Value();
      if (s == "string") opts.typed_fallback = TypedFallback::String;
      else if (s == "null") opts.typed_fallback = TypedFallback::Null;
    }
  }
}

class GetNextColumnarBatchWorker : public AsyncWorker {
 public:
  GetNextColumnarBatchWorker(Napi::Env env, StreamingColumnarParser* parser)
      : AsyncWorker(env),
        deferred_(Promise::Deferred::New(env)),
        parser_(parser),
        result_kind_(ColumnarResultKind::Done) {}

  Promise GetPromise() { return deferred_.Promise(); }

  void Execute() override {
    ColumnarBatchResult result;
    if (!parser_->queue().pop(result)) {
      result_kind_ = ColumnarResultKind::Cancelled;
      return;
    }
    result_kind_ = result.kind;
    if (result.kind == ColumnarResultKind::Error) {
      SetError(result.error_message);
      return;
    }
    if (result.kind == ColumnarResultKind::Batch) {
      batch_ = std::move(result.batch);
    }
  }

  void OnOK() override {
    if (result_kind_ == ColumnarResultKind::Done ||
        result_kind_ == ColumnarResultKind::Cancelled) {
      deferred_.Resolve(Env().Undefined());
      return;
    }
    deferred_.Resolve(ColumnarBatchToValue(Env(), batch_));
  }

  void OnError(const Error& e) override { deferred_.Reject(e.Value()); }

 private:
  Promise::Deferred deferred_;
  StreamingColumnarParser* parser_;
  ColumnarResultKind result_kind_;
  ColumnarBatch batch_;
};

static Value CreateColumnarParser(const CallbackInfo& info) {
  Env env = info.Env();
  if (info.Length() < 1 || !info[0].IsString()) {
    TypeError::New(env, "Expected path (string) as first argument")
        .ThrowAsJavaScriptException();
    return env.Null();
  }
  std::string path = info[0].As<String>().Utf8Value();

  ColumnarOptions opts;
  if (info.Length() >= 2 && info[1].IsObject()) {
    ParseColumnarOptions(env, info[1].As<Object>(), opts);
  }

  std::size_t max_queue = 2;
  bool use_mmap = false;
  std::size_t read_buffer_size = 0;
  if (info.Length() >= 2 && info[1].IsObject()) {
    Object options = info[1].As<Object>();
    if (options.Has("maxQueueBatches")) {
      Value v = options.Get("maxQueueBatches");
      if (v.IsNumber()) {
        double n = v.As<Number>().DoubleValue();
        if (n >= 1 && n <= 256) max_queue = static_cast<std::size_t>(n);
      }
    }
    if (options.Has("useMmap")) {
      Value v = options.Get("useMmap");
      if (v.IsBoolean()) use_mmap = v.As<Boolean>().Value();
    }
    if (options.Has("readBufferSize")) {
      Value v = options.Get("readBufferSize");
      if (v.IsNumber()) {
        double n = v.As<Number>().DoubleValue();
        if (n >= 4096 && n <= 64 * 1024 * 1024)
          read_buffer_size = static_cast<std::size_t>(n);
      }
    }
  }

  try {
    auto* parser = new StreamingColumnarParser(path, opts, max_queue, use_mmap,
                                               read_buffer_size);
    return External<StreamingColumnarParser>::New(env, parser);
  } catch (const std::exception& e) {
    Error::New(env, std::string("Failed to create columnar parser: ") + e.what())
        .ThrowAsJavaScriptException();
    return env.Null();
  }
}

static Value GetNextColumnarBatch(const CallbackInfo& info) {
  Env env = info.Env();
  if (info.Length() < 1 || !info[0].IsExternal()) {
    TypeError::New(env, "Expected parser (external) as first argument")
        .ThrowAsJavaScriptException();
    return env.Null();
  }
  auto* parser =
      info[0].As<External<StreamingColumnarParser>>().Data();
  auto* worker = new GetNextColumnarBatchWorker(env, parser);
  worker->Queue();
  return worker->GetPromise();
}

static Value DestroyColumnarParser(const CallbackInfo& info) {
  Env env = info.Env();
  if (info.Length() < 1 || !info[0].IsExternal()) {
    TypeError::New(env, "Expected parser (external) as first argument")
        .ThrowAsJavaScriptException();
    return env.Undefined();
  }
  auto* parser = info[0].As<External<StreamingColumnarParser>>().Data();
  parser->stop();
  delete parser;
  return env.Undefined();
}

// --- XLSX API ---

static Value XlsxBatchToValue(Env env, const XlsxBatch& batch) {
  Object obj = Object::New(env);
  Array headers = Array::New(env, batch.headers.size());
  for (std::size_t i = 0; i < batch.headers.size(); ++i) {
    headers[i] = String::New(env, batch.headers[i]);
  }
  obj.Set("headers", headers);
  obj.Set("rowsCount", Number::New(env, static_cast<double>(batch.rowsCount())));

  if (batch.columnar) {
    const ColumnarBatch& col = batch.columnar_batch;
    Object columns = Object::New(env);
    Object nullMask = Object::New(env);
    for (const auto& pair : col.columns) {
      const std::string& name = pair.first;
      const ColumnarColumn& col_col = pair.second;
      switch (col_col.type) {
        case ColumnType::String: {
          Array arr = Array::New(env, col_col.strings.size());
          for (std::size_t i = 0; i < col_col.strings.size(); ++i) {
            arr[i] = String::New(env, col_col.strings[i]);
          }
          columns.Set(name, arr);
          break;
        }
        case ColumnType::Int32: {
          Int32Array arr = Int32Array::New(env, col_col.int32_data->size());
          std::memcpy(arr.Data(), col_col.int32_data->data(),
                      col_col.int32_data->size() * sizeof(std::int32_t));
          columns.Set(name, arr);
          if (col_col.null_mask) {
            Uint8Array nm = Uint8Array::New(env, col_col.null_mask->size());
            std::memcpy(nm.Data(), col_col.null_mask->data(), col_col.null_mask->size());
            nullMask.Set(name, nm);
          }
          break;
        }
        case ColumnType::Int64: {
          BigInt64Array arr = BigInt64Array::New(env, col_col.int64_data->size());
          std::memcpy(arr.Data(), col_col.int64_data->data(),
                      col_col.int64_data->size() * sizeof(std::int64_t));
          columns.Set(name, arr);
          if (col_col.null_mask) {
            Uint8Array nm = Uint8Array::New(env, col_col.null_mask->size());
            std::memcpy(nm.Data(), col_col.null_mask->data(), col_col.null_mask->size());
            nullMask.Set(name, nm);
          }
          break;
        }
        case ColumnType::Float64: {
          Float64Array arr = Float64Array::New(env, col_col.float64_data->size());
          std::memcpy(arr.Data(), col_col.float64_data->data(),
                      col_col.float64_data->size() * sizeof(double));
          columns.Set(name, arr);
          if (col_col.null_mask) {
            Uint8Array nm = Uint8Array::New(env, col_col.null_mask->size());
            std::memcpy(nm.Data(), col_col.null_mask->data(), col_col.null_mask->size());
            nullMask.Set(name, nm);
          }
          break;
        }
        case ColumnType::Bool: {
          Uint8Array arr = Uint8Array::New(env, col_col.bool_data->size());
          std::memcpy(arr.Data(), col_col.bool_data->data(), col_col.bool_data->size());
          columns.Set(name, arr);
          if (col_col.null_mask) {
            Uint8Array nm = Uint8Array::New(env, col_col.null_mask->size());
            std::memcpy(nm.Data(), col_col.null_mask->data(), col_col.null_mask->size());
            nullMask.Set(name, nm);
          }
          break;
        }
      }
    }
    obj.Set("rows", columns);
    bool hasNullMask = false;
    for (const auto& pair : col.columns) {
      if (pair.second.null_mask && !pair.second.null_mask->empty()) {
        hasNullMask = true;
        break;
      }
    }
    if (hasNullMask) obj.Set("nullMask", nullMask);
  } else {
    Array rowsArr = Array::New(env, batch.rows.size());
    for (std::size_t i = 0; i < batch.rows.size(); ++i) {
      Array rowArr = Array::New(env, batch.rows[i].size());
      for (std::size_t j = 0; j < batch.rows[i].size(); ++j) {
        rowArr[j] = String::New(env, batch.rows[i][j]);
      }
      rowsArr[i] = rowArr;
    }
    obj.Set("rows", rowsArr);
  }
  return obj;
}

static void ParseXlsxOptions(Env env, Object options, XlsxOptions& opts) {
  if (options.Has("sheet")) {
    Value s = options.Get("sheet");
    if (s.IsNumber()) {
      opts.sheet_index = static_cast<int>(s.As<Number>().DoubleValue());
      opts.sheet_name.clear();
    } else if (s.IsString()) {
      opts.sheet_name = s.As<String>().Utf8Value();
      opts.sheet_index = 0;
    }
  }
  if (options.Has("headers")) {
    Value h = options.Get("headers");
    if (h.IsBoolean()) opts.headers = h.As<Boolean>().Value();
  }
  if (options.Has("batchSize")) {
    Value b = options.Get("batchSize");
    if (b.IsNumber()) {
      double n = b.As<Number>().DoubleValue();
      if (n >= 1 && n <= 10000000)
        opts.batch_size = static_cast<std::size_t>(n);
    }
  }
  if (options.Has("select")) {
    Value sel = options.Get("select");
    if (sel.IsArray()) {
      Array arr = sel.As<Array>();
      opts.select.clear();
      for (uint32_t i = 0; i < arr.Length(); ++i) {
        Value v = arr[i];
        if (v.IsString()) opts.select.push_back(v.As<String>().Utf8Value());
      }
    }
  }
  if (options.Has("schema")) {
    Value sch = options.Get("schema");
    if (sch.IsObject()) {
      Object schemaObj = sch.As<Object>();
      Array keys = schemaObj.GetPropertyNames();
      for (uint32_t i = 0; i < keys.Length(); ++i) {
        std::string key = keys.Get(i).As<String>().Utf8Value();
        Value v = schemaObj.Get(key);
        if (v.IsString()) {
          std::string t = v.As<String>().Utf8Value();
          if (t == "string") opts.schema[key] = ColumnType::String;
          else if (t == "int32") opts.schema[key] = ColumnType::Int32;
          else if (t == "int64") opts.schema[key] = ColumnType::Int64;
          else if (t == "float64") opts.schema[key] = ColumnType::Float64;
          else if (t == "bool") opts.schema[key] = ColumnType::Bool;
        }
      }
    }
  }
  if (options.Has("nullValues")) {
    Value nv = options.Get("nullValues");
    if (nv.IsArray()) {
      Array arr = nv.As<Array>();
      opts.null_values.clear();
      for (uint32_t i = 0; i < arr.Length(); ++i) {
        Value v = arr[i];
        if (v.IsString()) opts.null_values.push_back(v.As<String>().Utf8Value());
      }
    }
  }
  if (options.Has("trim")) {
    Value t = options.Get("trim");
    if (t.IsBoolean()) opts.trim = t.As<Boolean>().Value();
  }
  if (options.Has("typedFallback")) {
    Value tf = options.Get("typedFallback");
    if (tf.IsString()) {
      std::string s = tf.As<String>().Utf8Value();
      if (s == "string") opts.typed_fallback = TypedFallback::String;
      else if (s == "null") opts.typed_fallback = TypedFallback::Null;
    }
  }
}

class GetNextXlsxBatchWorker : public AsyncWorker {
 public:
  GetNextXlsxBatchWorker(Napi::Env env, StreamingXlsxParser* parser)
      : AsyncWorker(env),
        deferred_(Promise::Deferred::New(env)),
        parser_(parser),
        result_kind_(XlsxResultKind::Done) {}

  Promise GetPromise() { return deferred_.Promise(); }

  void Execute() override {
    XlsxBatchResult result;
    if (!parser_->queue().pop(result)) {
      result_kind_ = XlsxResultKind::Cancelled;
      return;
    }
    result_kind_ = result.kind;
    if (result.kind == XlsxResultKind::Error) {
      SetError(result.error_message);
      return;
    }
    if (result.kind == XlsxResultKind::Batch) {
      batch_ = std::move(result.batch);
    }
  }

  void OnOK() override {
    if (result_kind_ == XlsxResultKind::Done ||
        result_kind_ == XlsxResultKind::Cancelled) {
      deferred_.Resolve(Env().Undefined());
      return;
    }
    deferred_.Resolve(XlsxBatchToValue(Env(), batch_));
  }

  void OnError(const Error& e) override { deferred_.Reject(e.Value()); }

 private:
  Promise::Deferred deferred_;
  StreamingXlsxParser* parser_;
  XlsxResultKind result_kind_;
  XlsxBatch batch_;
};

static Value CreateXlsxParser(const CallbackInfo& info) {
  Env env = info.Env();
  if (info.Length() < 1 || !info[0].IsString()) {
    TypeError::New(env, "Expected path (string) as first argument")
        .ThrowAsJavaScriptException();
    return env.Null();
  }
  std::string path = info[0].As<String>().Utf8Value();

  XlsxOptions opts;
  if (info.Length() >= 2 && info[1].IsObject()) {
    ParseXlsxOptions(env, info[1].As<Object>(), opts);
  }

  try {
    auto* parser = new StreamingXlsxParser(path, opts);
    return External<StreamingXlsxParser>::New(env, parser);
  } catch (const std::exception& e) {
    Error::New(env, std::string("Failed to create XLSX parser: ") + e.what())
        .ThrowAsJavaScriptException();
    return env.Null();
  }
}

static Value GetNextXlsxBatch(const CallbackInfo& info) {
  Env env = info.Env();
  if (info.Length() < 1 || !info[0].IsExternal()) {
    TypeError::New(env, "Expected parser (external) as first argument")
        .ThrowAsJavaScriptException();
    return env.Null();
  }
  auto* parser = info[0].As<External<StreamingXlsxParser>>().Data();
  auto* worker = new GetNextXlsxBatchWorker(env, parser);
  worker->Queue();
  return worker->GetPromise();
}

static Value DestroyXlsxParser(const CallbackInfo& info) {
  Env env = info.Env();
  if (info.Length() < 1 || !info[0].IsExternal()) {
    TypeError::New(env, "Expected parser (external) as first argument")
        .ThrowAsJavaScriptException();
    return env.Undefined();
  }
  auto* parser = info[0].As<External<StreamingXlsxParser>>().Data();
  parser->stop();
  delete parser;
  return env.Undefined();
}

static Value GetColumnarParserMetrics(const CallbackInfo& info) {
  Env env = info.Env();
  if (info.Length() < 1 || !info[0].IsExternal()) {
    TypeError::New(env, "Expected parser (external) as first argument")
        .ThrowAsJavaScriptException();
    return env.Null();
  }
  auto* parser = info[0].As<External<StreamingColumnarParser>>().Data();
  const PipelineMetrics& m = parser->metrics();
  Object obj = Object::New(env);
  obj.Set("bytes_read", Number::New(env, static_cast<double>(m.bytes_read.load())));
  obj.Set("rows_parsed", Number::New(env, static_cast<double>(m.rows_parsed.load())));
  obj.Set("batches_emitted", Number::New(env, static_cast<double>(m.batches_emitted.load())));
  obj.Set("queue_wait_ns", Number::New(env, static_cast<double>(m.queue_wait_ns.load())));
  obj.Set("parse_time_ns", Number::New(env, static_cast<double>(m.parse_time_ns.load())));
  obj.Set("read_time_ns", Number::New(env, static_cast<double>(m.read_time_ns.load())));
  obj.Set("build_time_ns", Number::New(env, static_cast<double>(m.build_time_ns.load())));
  obj.Set("emit_time_ns", Number::New(env, static_cast<double>(m.emit_time_ns.load())));
  obj.Set("arena_resizes", Number::New(env, static_cast<double>(m.arena_resizes.load())));
  obj.Set("batch_allocations", Number::New(env, static_cast<double>(m.batch_allocations.load())));
  obj.Set("arena_bytes_allocated", Number::New(env, static_cast<double>(m.arena_bytes_allocated.load())));
  obj.Set("arena_blocks", Number::New(env, static_cast<double>(m.arena_blocks.load())));
  obj.Set("arena_resets", Number::New(env, static_cast<double>(m.arena_resets.load())));
  obj.Set("peak_arena_usage", Number::New(env, static_cast<double>(m.peak_arena_usage.load())));
  return obj;
}

static Object Init(Env env, Object exports) {
  exports.Set("createParser", Function::New(env, CreateParser));
  exports.Set("getNextBatch", Function::New(env, GetNextBatch));
  exports.Set("destroyParser", Function::New(env, DestroyParser));
  exports.Set("getParserMetrics", Function::New(env, GetParserMetrics));
  exports.Set("createColumnarParser", Function::New(env, CreateColumnarParser));
  exports.Set("getNextColumnarBatch", Function::New(env, GetNextColumnarBatch));
  exports.Set("destroyColumnarParser", Function::New(env, DestroyColumnarParser));
  exports.Set("getColumnarParserMetrics", Function::New(env, GetColumnarParserMetrics));
  exports.Set("createXlsxParser", Function::New(env, CreateXlsxParser));
  exports.Set("getNextXlsxBatch", Function::New(env, GetNextXlsxBatch));
  exports.Set("destroyXlsxParser", Function::New(env, DestroyXlsxParser));
  return exports;
}

NODE_API_MODULE(ultratab, Init)

}  // namespace ultratab
