/*
 * Copyright 2023 The Fury Authors
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

#include "gtest/gtest.h"
#include <type_traits>

#include "fury/encoder/row_encode_trait.h"
#include "src/fury/encoder/row_encoder.h"
#include "src/fury/row/writer.h"

namespace fury {

namespace test2 {

struct A {
  float a;
  std::string b;
};

FURY_FIELD_INFO(A, a, b);

struct B {
  int x;
  A y;
};

FURY_FIELD_INFO(B, x, y);

TEST(RowEncoder, Simple) {
  B v{233, {1.23, "hello"}};

  encoder::RowEncoder<B> enc;

  auto &schema = enc.GetSchema();
  ASSERT_EQ(schema.field_names(), (std::vector<std::string>{"x", "y"}));
  ASSERT_EQ(schema.field(0)->type()->name(), "int32");
  ASSERT_EQ(schema.field(1)->type()->name(), "struct");
  ASSERT_EQ(schema.field(1)->type()->field(0)->name(), "a");
  ASSERT_EQ(schema.field(1)->type()->field(1)->name(), "b");
  ASSERT_EQ(schema.field(1)->type()->field(0)->type()->name(), "float");
  ASSERT_EQ(schema.field(1)->type()->field(1)->type()->name(), "utf8");

  enc.Encode(v);

  auto row = enc.GetWriter().ToRow();
  ASSERT_EQ(row->GetInt32(0), 233);
  auto y_row = row->GetStruct(1);
  ASSERT_EQ(y_row->GetString(1), "hello");
  ASSERT_FLOAT_EQ(y_row->GetFloat(0), 1.23);
}

} // namespace test2
} // namespace fury
