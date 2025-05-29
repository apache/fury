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

#include "gtest/gtest.h"

#include "fory/columnar/arrow_writer.h"
#include "fory/row/writer.h"
#include <limits>
#include <memory>
#include <vector>

namespace fory {
namespace columnar {

TEST(ARROW_WRITER, BASE_TYPE_TO_RECORD_BATCH) {
  auto f1 = arrow::field("f0", arrow::int32());
  auto f2 = arrow::field("f1", arrow::int64());
  auto f3 = arrow::field("f2", arrow::float64());
  std::vector<std::shared_ptr<arrow::Field>> fields = {f1, f2, f3};
  auto schema = arrow::schema(fields);
  std::vector<std::shared_ptr<Row>> rows;
  int row_nums = 100;
  RowWriter row_writer(schema);
  for (int i = 0; i < row_nums; ++i) {
    std::shared_ptr<Buffer> buffer;
    AllocateBuffer(16, &buffer);
    row_writer.SetBuffer(buffer);
    row_writer.Reset();
    row_writer.Write(0, std::numeric_limits<int>::max());
    row_writer.Write(1, std::numeric_limits<int64_t>::max());
    row_writer.Write(2, std::numeric_limits<double>::max());
    rows.push_back(row_writer.ToRow());
  }

  std::shared_ptr<ArrowWriter> arrow_writer;
  EXPECT_TRUE(
      ArrowWriter::Make(schema, ::arrow::default_memory_pool(), &arrow_writer)
          .ok());
  for (auto &row : rows) {
    EXPECT_TRUE(arrow_writer->Write(row).ok());
  }
  std::shared_ptr<::arrow::RecordBatch> record_batch;
  EXPECT_TRUE(arrow_writer->Finish(&record_batch).ok());
  EXPECT_TRUE(record_batch->Validate().ok());
  EXPECT_EQ(record_batch->num_columns(), schema->num_fields());
  EXPECT_EQ(record_batch->num_rows(), row_nums);
  // std::cout << record_batch->column(0)->ToString() << std::endl;
}

TEST(ARROW_WRITER, ARRAY_TYPE_TO_RECORD_BATCH) {
  auto f0 = arrow::field("f0", arrow::int32());
  auto f1 = arrow::field("f1", arrow::list(arrow::int32()));
  auto schema = arrow::schema({f0, f1});
  std::vector<std::shared_ptr<Row>> rows;
  int row_nums = 100;
  RowWriter row_writer(schema);
  ArrayWriter array_writer(
      std::dynamic_pointer_cast<arrow::ListType>(schema->field(1)->type()),
      &row_writer);
  for (int i = 0; i < row_nums; ++i) {
    std::shared_ptr<Buffer> buffer;
    AllocateBuffer(16, &buffer);
    row_writer.SetBuffer(buffer);
    row_writer.Reset();
    row_writer.Write(0, std::numeric_limits<int>::max());

    int start = row_writer.cursor();
    int array_elements = 50;
    array_writer.Reset(array_elements);
    for (int j = 0; j < array_elements; ++j) {
      array_writer.Write(j, std::numeric_limits<int>::min());
    }
    row_writer.SetOffsetAndSize(1, start, row_writer.cursor() - start);
    rows.push_back(row_writer.ToRow());
  }
  // std::cout << rows[0]->ToString() << std::endl;

  std::shared_ptr<ArrowWriter> arrow_writer;
  EXPECT_TRUE(
      ArrowWriter::Make(schema, ::arrow::default_memory_pool(), &arrow_writer)
          .ok());
  for (auto &row : rows) {
    EXPECT_TRUE(arrow_writer->Write(row).ok());
  }
  std::shared_ptr<::arrow::RecordBatch> record_batch;
  EXPECT_TRUE(arrow_writer->Finish(&record_batch).ok());
  EXPECT_TRUE(record_batch->Validate().ok());
  EXPECT_EQ(record_batch->num_columns(), schema->num_fields());
  EXPECT_EQ(record_batch->num_rows(), row_nums);
  // std::cout << record_batch->column(1)->ToString() << std::endl;
}

typedef struct {
  std::vector<std::vector<int>> indexes;
  std::vector<int> values;
  std::vector<int> shape;
} SparseTensor;

TEST(ARROW_WRITER, SPARSE_TENSOR_TO_RECORD_BATCH) {
  auto indexes =
      arrow::field("indexes", arrow::list(arrow::list(arrow::int32())));
  auto values = arrow::field("values", arrow::list(arrow::int32()));
  auto shape = arrow::field("shape", arrow::list(arrow::int32()));
  auto sparse_tensor_type = arrow::struct_({indexes, values, shape});
  auto sparse_tensor_field = arrow::field("sparse_tensor", sparse_tensor_type);
  auto schema = arrow::schema({sparse_tensor_field});

  std::vector<std::shared_ptr<Row>> rows;
  RowWriter row_writer(schema);
  auto struct_type =
      std::dynamic_pointer_cast<arrow::StructType>(schema->field(0)->type());
  RowWriter tensor_struct_writer(arrow::schema(struct_type->fields()),
                                 &row_writer);
  ArrayWriter indexes_writer(
      std::dynamic_pointer_cast<arrow::ListType>(struct_type->field(0)->type()),
      &tensor_struct_writer);
  ArrayWriter values_writer(
      std::dynamic_pointer_cast<arrow::ListType>(struct_type->field(1)->type()),
      &tensor_struct_writer);
  int row_nums = 100;
  int tensor_values_num = 3;
  SparseTensor sparse_tensor = {{{0, 0}, {1, 1}, {1, 2}}, {1, 2, 3}, {3, 4}};
  for (int i = 0; i < row_nums; ++i) {
    std::shared_ptr<Buffer> buffer;
    AllocateBuffer(16, &buffer);
    row_writer.SetBuffer(buffer);
    row_writer.Reset();

    auto tensor_struct_start = row_writer.cursor();
    tensor_struct_writer.Reset();

    auto indexes_start = indexes_writer.cursor();
    indexes_writer.Reset(tensor_values_num);
    for (int j = 0; j < tensor_values_num; ++j) {
      indexes_writer.WriteArray(j, ArrayData::From(sparse_tensor.indexes[j]));
    }
    // std::cout << indexes_writer.CopyToArrayData()->ToString() << std::endl;
    tensor_struct_writer.SetOffsetAndSize(
        0, indexes_start, tensor_struct_writer.cursor() - tensor_struct_start);

    auto tensor_values_start = values_writer.cursor();
    values_writer.Reset(tensor_values_num);
    for (int j = 0; j < tensor_values_num; ++j) {
      values_writer.Write(j, sparse_tensor.values[j]);
    }
    // std::cout << values_writer.CopyToArrayData()->ToString() << std::endl;
    tensor_struct_writer.SetOffsetAndSize(1, tensor_values_start,
                                          tensor_struct_writer.cursor() -
                                              tensor_struct_start);

    tensor_struct_writer.WriteArray(2, ArrayData::From(sparse_tensor.shape));

    row_writer.SetOffsetAndSize(0, tensor_struct_start,
                                row_writer.cursor() - tensor_struct_start);
    rows.push_back(row_writer.ToRow());
  }
  // std::cout << rows[0]->ToString() << std::endl;
  std::shared_ptr<ArrowWriter> arrow_writer;
  EXPECT_TRUE(
      ArrowWriter::Make(schema, ::arrow::default_memory_pool(), &arrow_writer)
          .ok());
  for (auto &row : rows) {
    EXPECT_TRUE(arrow_writer->Write(row).ok());
  }
  std::shared_ptr<::arrow::RecordBatch> record_batch;
  EXPECT_TRUE(arrow_writer->Finish(&record_batch).ok());
  EXPECT_TRUE(record_batch->Validate().ok());
  EXPECT_EQ(record_batch->num_columns(), schema->num_fields());
  EXPECT_EQ(record_batch->num_rows(), row_nums);
  // std::cout << record_batch->column(0)->ToString() << std::endl;
}

} // namespace columnar
} // namespace fory

int main(int argc, char **argv) {
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
