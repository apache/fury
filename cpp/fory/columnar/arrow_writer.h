/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

#pragma once

#include "arrow/api.h"
#include "fory/row/row.h"
#include "fory/util/logging.h"
#include <utility>

namespace fory {
namespace columnar {

class ArrowArrayWriter;

class ArrowWriter {
public:
  static ::arrow::Status
  Make(const std::shared_ptr<arrow::Schema> &arrow_schema,
       ::arrow::MemoryPool *pool, std::shared_ptr<ArrowWriter> *writer);

  ::arrow::Status Write(const std::shared_ptr<Row> &row);

  ::arrow::Status Finish(std::shared_ptr<::arrow::RecordBatch> *record_batch);

  void Reset();

private:
  explicit ArrowWriter(std::shared_ptr<::arrow::Schema> arrow_schema,
                       ::arrow::MemoryPool *pool)
      : pool_(pool), arrow_schema_(std::move(arrow_schema)) {
    FORY_CHECK(pool_ != nullptr);
  }

  static ::arrow::Status
  createArrayWriter(const std::shared_ptr<::arrow::DataType> &type,
                    ::arrow::MemoryPool *pool,
                    std::unique_ptr<ArrowArrayWriter> *arrow_array_writer);

  ::arrow::MemoryPool *pool_;
  const std::shared_ptr<::arrow::Schema> arrow_schema_;
  std::vector<std::unique_ptr<ArrowArrayWriter>> column_writers_;
  int num_rows_ = 0;
};

class ArrowArrayWriter {
public:
  virtual ~ArrowArrayWriter() = default;

  ::arrow::Status Write(const std::shared_ptr<fory::Getter> &getter, int i);

  virtual ::arrow::Status AppendNull() { return builder()->AppendNull(); }

  virtual ::arrow::Status AppendValue(std::shared_ptr<Getter> getter,
                                      int i) = 0;

  virtual ::arrow::Status Finish(std::shared_ptr<::arrow::Array> *array) {
    return builder()->Finish(array);
  }

  virtual void Reset() { builder()->Reset(); }

  virtual std::shared_ptr<::arrow::ArrayBuilder> builder() = 0;
};

#define _NUMERIC_TYPE_WRITER_DECL(KLASS)                                       \
  class KLASS##Writer : public ArrowArrayWriter {                              \
  public:                                                                      \
    explicit KLASS##Writer(::arrow::MemoryPool *pool)                          \
        : builder_(std::make_shared<::arrow::KLASS##Builder>(pool)){};         \
    ::arrow::Status AppendNull() override { return builder_->AppendNull(); }   \
    ::arrow::Status AppendValue(std::shared_ptr<Getter> getter,                \
                                int i) override {                              \
      return builder_->Append(getter->Get##KLASS(i));                          \
    }                                                                          \
    std::shared_ptr<::arrow::ArrayBuilder> builder() override {                \
      return std::static_pointer_cast<::arrow::ArrayBuilder>(builder_);        \
    }                                                                          \
                                                                               \
  private:                                                                     \
    std::shared_ptr<::arrow::KLASS##Builder> builder_;                         \
  };

_NUMERIC_TYPE_WRITER_DECL(Int8)

_NUMERIC_TYPE_WRITER_DECL(Int16)

_NUMERIC_TYPE_WRITER_DECL(Int32)

_NUMERIC_TYPE_WRITER_DECL(Int64)

_NUMERIC_TYPE_WRITER_DECL(Float)

_NUMERIC_TYPE_WRITER_DECL(Double)

class BooleanWriter : public ArrowArrayWriter {
public:
  explicit BooleanWriter(::arrow::MemoryPool *pool)
      : builder_(std::make_shared<::arrow::BooleanBuilder>(pool)) {}

  ::arrow::Status AppendValue(std::shared_ptr<Getter> getter, int i) override {
    return builder_->Append(getter->GetBoolean(i));
  }

  std::shared_ptr<::arrow::ArrayBuilder> builder() override {
    return std::static_pointer_cast<::arrow::ArrayBuilder>(builder_);
  }

private:
  std::shared_ptr<::arrow::BooleanBuilder> builder_;
};

class DateWriter : public ArrowArrayWriter {
public:
  explicit DateWriter(::arrow::MemoryPool *pool)
      : builder_(std::make_shared<::arrow::Date32Builder>(pool)) {}

  ::arrow::Status AppendValue(std::shared_ptr<Getter> getter, int i) override {
    return builder_->Append(getter->GetInt32(i));
  }

  std::shared_ptr<::arrow::ArrayBuilder> builder() override {
    return std::static_pointer_cast<::arrow::ArrayBuilder>(builder_);
  }

private:
  std::shared_ptr<::arrow::Date32Builder> builder_;
};

class Time32Writer : public ArrowArrayWriter {
public:
  explicit Time32Writer(const std::shared_ptr<::arrow::DataType> &type,
                        ::arrow::MemoryPool *pool)
      : builder_(std::make_shared<::arrow::Time32Builder>(type, pool)) {}

  ::arrow::Status AppendValue(std::shared_ptr<Getter> getter, int i) override {
    return builder_->Append(getter->GetInt32(i));
  }

  std::shared_ptr<::arrow::ArrayBuilder> builder() override {
    return std::static_pointer_cast<::arrow::ArrayBuilder>(builder_);
  }

private:
  std::shared_ptr<::arrow::Time32Builder> builder_;
};

class Time64Writer : public ArrowArrayWriter {
public:
  explicit Time64Writer(const std::shared_ptr<::arrow::DataType> &type,
                        ::arrow::MemoryPool *pool)
      : builder_(std::make_shared<::arrow::Time64Builder>(type, pool)) {}

  ::arrow::Status AppendValue(std::shared_ptr<Getter> getter, int i) override {
    return builder_->Append(getter->GetInt64(i));
  }

  std::shared_ptr<::arrow::ArrayBuilder> builder() override {
    return std::static_pointer_cast<::arrow::ArrayBuilder>(builder_);
  }

private:
  std::shared_ptr<::arrow::Time64Builder> builder_;
};

class TimestampWriter : public ArrowArrayWriter {
public:
  explicit TimestampWriter(const std::shared_ptr<::arrow::DataType> &type,
                           ::arrow::MemoryPool *pool)
      : builder_(std::make_shared<::arrow::TimestampBuilder>(type, pool)) {}

  ::arrow::Status AppendValue(std::shared_ptr<Getter> getter, int i) override {
    return builder_->Append(getter->GetInt64(i));
  }

  std::shared_ptr<::arrow::ArrayBuilder> builder() override {
    return std::static_pointer_cast<::arrow::ArrayBuilder>(builder_);
  }

private:
  std::shared_ptr<::arrow::TimestampBuilder> builder_;
};

class BinaryWriter : public ArrowArrayWriter {
public:
  explicit BinaryWriter(::arrow::MemoryPool *pool)
      : builder_(std::make_shared<::arrow::BinaryBuilder>(pool)) {}

  ::arrow::Status AppendValue(std::shared_ptr<Getter> getter, int i) override {
    uint8_t *bytes;
    int size = getter->GetBinary(i, &bytes);
    return builder_->Append(bytes, size);
  }

  std::shared_ptr<::arrow::ArrayBuilder> builder() override {
    return std::static_pointer_cast<::arrow::ArrayBuilder>(builder_);
  }

private:
  std::shared_ptr<::arrow::BinaryBuilder> builder_;
};

class StringWriter : public ArrowArrayWriter {
public:
  explicit StringWriter(::arrow::MemoryPool *pool)
      : builder_(std::make_shared<::arrow::StringBuilder>(pool)) {}

  ::arrow::Status AppendValue(std::shared_ptr<Getter> getter, int i) override {
    uint8_t *bytes;
    int size = getter->GetBinary(i, &bytes);
    return builder_->Append(bytes, size);
  }

  std::shared_ptr<::arrow::ArrayBuilder> builder() override {
    return std::static_pointer_cast<::arrow::ArrayBuilder>(builder_);
  }

private:
  std::shared_ptr<::arrow::StringBuilder> builder_;
};

class ListWriter : public ArrowArrayWriter {
public:
  ListWriter(const std::shared_ptr<::arrow::DataType> &type,
             ::arrow::MemoryPool *pool,
             std::unique_ptr<ArrowArrayWriter> elem_writer);

  ::arrow::Status AppendValue(std::shared_ptr<Getter> getter, int i) override;

  std::shared_ptr<::arrow::ArrayBuilder> builder() override {
    return std::static_pointer_cast<::arrow::ArrayBuilder>(builder_);
  }

  void Reset() override {
    builder_->Reset();
    elem_writer_->Reset();
  }

protected:
  std::shared_ptr<::arrow::ListBuilder> builder_;
  std::unique_ptr<ArrowArrayWriter> elem_writer_;
};

class StructWriter : public ArrowArrayWriter {
public:
  StructWriter(const std::shared_ptr<::arrow::DataType> &type,
               ::arrow::MemoryPool *pool,
               std::vector<std::unique_ptr<ArrowArrayWriter>> &&field_writers);

  ::arrow::Status AppendValue(std::shared_ptr<Getter> getter, int i) override;

  std::shared_ptr<::arrow::ArrayBuilder> builder() override {
    return std::static_pointer_cast<::arrow::ArrayBuilder>(builder_);
  }

  void Reset() override {
    builder_->Reset();
    for (auto &array_writer : field_writers_) {
      array_writer->Reset();
    }
  }

private:
  std::shared_ptr<::arrow::StructBuilder> builder_;
  std::vector<std::unique_ptr<ArrowArrayWriter>> field_writers_;
};

class MapWriter : public ArrowArrayWriter {
public:
  MapWriter(const std::shared_ptr<::arrow::DataType> &type,
            ::arrow::MemoryPool *pool,
            std::unique_ptr<ArrowArrayWriter> key_writer,
            std::unique_ptr<ArrowArrayWriter> item_writer);

  ::arrow::Status AppendValue(std::shared_ptr<Getter> getter, int i) override;

  std::shared_ptr<::arrow::ArrayBuilder> builder() override {
    return std::static_pointer_cast<::arrow::ArrayBuilder>(builder_);
  }

  void Reset() override {
    builder_->Reset();
    key_writer_->Reset();
    item_writer_->Reset();
  }

private:
  std::shared_ptr<::arrow::MapBuilder> builder_;
  std::unique_ptr<ArrowArrayWriter> key_writer_;
  std::unique_ptr<ArrowArrayWriter> item_writer_;
};

} // namespace columnar
} // namespace fory
