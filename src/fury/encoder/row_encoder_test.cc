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

#include "fury/encoder/row_encoder.h"

namespace fury {

namespace test {

struct A {
  int x;
  float y;
  bool z;
};

FURY_FIELD_INFO(A, x, y, z);

TEST(RowEncoder, Simple) {
  auto field_vector = encoder::RowEncodeTrait<A>::FieldVector();

  static_assert(std::is_same_v<decltype(field_vector), arrow::FieldVector>);

  ASSERT_EQ(field_vector.size(), 3);
  ASSERT_EQ(field_vector[0]->name(), "x");
  ASSERT_EQ(field_vector[1]->name(), "y");
  ASSERT_EQ(field_vector[2]->name(), "z");

  ASSERT_EQ(field_vector[0]->type()->name(), "int32");
  ASSERT_EQ(field_vector[1]->type()->name(), "float");
  ASSERT_EQ(field_vector[2]->type()->name(), "bool");

  RowWriter writer(arrow::schema(field_vector));
  writer.Reset();

  A a{233, 3.14, true};
  encoder::RowEncodeTrait<A>::Write(a, writer);

  auto row = writer.ToRow();
  ASSERT_EQ(row->GetInt32(0), 233);
  ASSERT_FLOAT_EQ(row->GetFloat(1), 3.14);
  ASSERT_EQ(row->GetBoolean(2), true);
}

struct B {
  int num;
  std::string str;
};

FURY_FIELD_INFO(B, num, str);

TEST(RowEncoder, String) {
  RowWriter writer(encoder::RowEncodeTrait<B>::Schema());
  writer.Reset();

  B b{233, "hello"};
  encoder::RowEncodeTrait<B>::Write(b, writer);

  auto row = writer.ToRow();
  ASSERT_EQ(row->GetString(1), "hello");
  ASSERT_EQ(row->GetInt32(0), 233);
}

struct C {
  const int a;
  volatile float b;
  bool c;
};

FURY_FIELD_INFO(C, a, b, c);

TEST(RowEncoder, Const) {
  RowWriter writer(encoder::RowEncodeTrait<C>::Schema());
  writer.Reset();

  C c{233, 1.1, true};
  encoder::RowEncodeTrait<C>::Write(c, writer);

  auto row = writer.ToRow();
  ASSERT_EQ(row->GetInt32(0), 233);
  ASSERT_FLOAT_EQ(row->GetFloat(1), 1.1);
  ASSERT_EQ(row->GetBoolean(2), true);
}

} // namespace test

} // namespace fury

int main(int argc, char **argv) {
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
