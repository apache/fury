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

#include "fory/columnar/arrow_writer.h"

namespace fory {
namespace columnar {

::arrow::Status
ArrowWriter::Make(const std::shared_ptr<::arrow::Schema> &arrow_schema,
                  ::arrow::MemoryPool *pool,
                  std::shared_ptr<ArrowWriter> *writer) {
  auto out = std::shared_ptr<ArrowWriter>(new ArrowWriter(arrow_schema, pool));
  for (auto &field : arrow_schema->fields()) {
    std::unique_ptr<ArrowArrayWriter> array_writer;
    RETURN_NOT_OK(createArrayWriter(field->type(), pool, &array_writer));
    out->column_writers_.push_back(std::move(array_writer));
  }
  *writer = out;
  return arrow::Status::OK();
}

::arrow::Status ArrowWriter::createArrayWriter(
    const std::shared_ptr<::arrow::DataType> &type, ::arrow::MemoryPool *pool,
    std::unique_ptr<ArrowArrayWriter> *arrow_array_writer) {
  ArrowArrayWriter *writer;
  switch (type->id()) {
  case ::arrow::Type::BOOL:
    writer = new BooleanWriter(pool);
    break;
  case ::arrow::Type::INT8:
    writer = new Int8Writer(pool);
    break;
  case ::arrow::Type::INT16:
    writer = new Int16Writer(pool);
    break;
  case ::arrow::Type::INT32:
    writer = new Int32Writer(pool);
    break;
  case ::arrow::Type::INT64:
    writer = new Int64Writer(pool);
    break;
  case ::arrow::Type::FLOAT:
    writer = new FloatWriter(pool);
    break;
  case ::arrow::Type::DOUBLE:
    writer = new DoubleWriter(pool);
    break;
  case ::arrow::Type::DECIMAL:
    return ::arrow::Status::NotImplemented("Unsupported type",
                                           type->ToString());
  case ::arrow::Type::DATE32:
    writer = new DateWriter(pool);
    break;
  case ::arrow::Type::TIME32:
    writer = new Time32Writer(type, pool);
    break;
  case ::arrow::Type::TIME64:
    writer = new Time64Writer(type, pool);
    break;
  case ::arrow::Type::TIMESTAMP:
    writer = new TimestampWriter(type, pool);
    break;
  case ::arrow::Type::BINARY:
    writer = new BinaryWriter(pool);
    break;
  case ::arrow::Type::STRING:
    writer = new StringWriter(pool);
    break;
  case ::arrow::Type::LIST: {
    std::unique_ptr<ArrowArrayWriter> elem_writer;
    RETURN_NOT_OK(createArrayWriter(
        std::dynamic_pointer_cast<::arrow::ListType>(type)->value_type(), pool,
        &elem_writer));
    writer = new ListWriter(type, pool, std::move(elem_writer));
    break;
  }
  case ::arrow::Type::MAP: {
    std::unique_ptr<ArrowArrayWriter> key_writer;
    RETURN_NOT_OK(createArrayWriter(
        std::dynamic_pointer_cast<::arrow::MapType>(type)->key_type(), pool,
        &key_writer));
    std::unique_ptr<ArrowArrayWriter> value_writer;
    RETURN_NOT_OK(createArrayWriter(
        std::dynamic_pointer_cast<::arrow::MapType>(type)->item_type(), pool,
        &value_writer));
    writer = new MapWriter(type, pool, std::move(key_writer),
                           std::move(value_writer));
    break;
  }
  case ::arrow::Type::STRUCT: {
    std::vector<std::unique_ptr<ArrowArrayWriter>> field_writers;
    auto struct_type = std::dynamic_pointer_cast<::arrow::StructType>(type);
    for (auto &field : struct_type->fields()) {
      std::unique_ptr<ArrowArrayWriter> field_writer;
      RETURN_NOT_OK(createArrayWriter(field->type(), pool, &field_writer));
      field_writers.push_back(std::move(field_writer));
    }
    writer = new StructWriter(type, pool, std::move(field_writers));
    break;
  }
  default:
    return ::arrow::Status::NotImplemented("Unsupported type",
                                           type->ToString());
  }
  *arrow_array_writer = std::unique_ptr<ArrowArrayWriter>(writer);
  return ::arrow::Status::OK();
}

::arrow::Status ArrowWriter::Write(const std::shared_ptr<Row> &row) {
  int num_fields = row->num_fields();
  for (int i = 0; i < num_fields; ++i) {
    auto &field_writer = column_writers_[i];
    RETURN_NOT_OK(field_writer->Write(row, i));
  }
  num_rows_++;
  return ::arrow::Status::OK();
}

::arrow::Status
ArrowWriter::Finish(std::shared_ptr<::arrow::RecordBatch> *record_batch) {
  std::vector<std::shared_ptr<::arrow::Array>> columns;
  for (auto &array_writer : column_writers_) {
    std::shared_ptr<::arrow::Array> array;
    RETURN_NOT_OK(array_writer->Finish(&array));
    columns.push_back(array);
  }
  *record_batch = ::arrow::RecordBatch::Make(arrow_schema_, num_rows_, columns);
  return ::arrow::Status::OK();
}

void ArrowWriter::Reset() {
  num_rows_ = 0;
  for (auto &array_writer : column_writers_) {
    array_writer->Reset();
  }
}

::arrow::Status
ArrowArrayWriter::Write(const std::shared_ptr<fory::Getter> &getter, int i) {
  ::arrow::Status status;
  if (getter->IsNullAt(i)) {
    status = AppendNull();
  } else {
    status = AppendValue(getter, i);
  }
  return status;
}

ListWriter::ListWriter(const std::shared_ptr<::arrow::DataType> &type,
                       ::arrow::MemoryPool *pool,
                       std::unique_ptr<ArrowArrayWriter> elem_writer) {
  builder_ = std::make_shared<::arrow::ListBuilder>(
      pool, elem_writer->builder(), type);
  elem_writer_ = std::move(elem_writer);
}

::arrow::Status ListWriter::AppendValue(std::shared_ptr<fory::Getter> getter,
                                        int i) {
  auto array = getter->GetArray(i);
  RETURN_NOT_OK(builder_->Append());
  auto num_elements = array->num_elements();
  for (int x = 0; x < num_elements; ++x) {
    RETURN_NOT_OK(elem_writer_->Write(array, x));
  }
  return ::arrow::Status::OK();
}

::arrow::Status StructWriter::AppendValue(std::shared_ptr<fory::Getter> getter,
                                          int i) {
  auto struct_data = getter->GetStruct(i);
  auto num_fields = struct_data->num_fields();
  RETURN_NOT_OK(builder_->Append());
  for (int x = 0; x < num_fields; ++x) {
    RETURN_NOT_OK(field_writers_[x]->Write(struct_data, x));
  }
  return ::arrow::Status::OK();
}

StructWriter::StructWriter(
    const std::shared_ptr<::arrow::DataType> &type, ::arrow::MemoryPool *pool,
    std::vector<std::unique_ptr<ArrowArrayWriter>> &&field_writers) {
  std::vector<std::shared_ptr<::arrow::ArrayBuilder>> field_builders;
  field_builders.reserve(field_writers.size());
  for (auto &field_writer : field_writers) {
    field_builders.push_back(field_writer->builder());
  }
  builder_ = std::make_shared<::arrow::StructBuilder>(
      type, pool, std::move(field_builders));
  field_writers_ = std::move(field_writers);
}

MapWriter::MapWriter(const std::shared_ptr<::arrow::DataType> &type,
                     ::arrow::MemoryPool *pool,
                     std::unique_ptr<ArrowArrayWriter> key_writer,
                     std::unique_ptr<ArrowArrayWriter> item_writer) {
  builder_ = std::make_shared<::arrow::MapBuilder>(
      pool, key_writer->builder(), item_writer->builder(), type);
  key_writer_ = std::move(key_writer);
  item_writer_ = std::move(item_writer);
}

::arrow::Status MapWriter::AppendValue(std::shared_ptr<fory::Getter> getter,
                                       int i) {
  auto map = getter->GetMap(i);
  auto key_array = map->keys_array();
  auto value_array = map->values_array();
  RETURN_NOT_OK(builder_->Append());
  auto num_elements = map->num_elements();
  for (int i = 0; i < num_elements; ++i) {
    RETURN_NOT_OK(key_writer_->Write(key_array, i));
    RETURN_NOT_OK(item_writer_->Write(value_array, i));
  }

  return ::arrow::Status::OK();
}

} // namespace columnar
} // namespace fory
