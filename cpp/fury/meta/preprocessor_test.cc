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

#include "fury/meta/preprocessor.h"

namespace fury {

TEST(Preprocessor, NArg) {
  static_assert(FURY_PP_NARG(x) == 1);
  static_assert(FURY_PP_NARG(x, y) == 2);
  static_assert(FURY_PP_NARG(x, y, z) == 3);
  static_assert(FURY_PP_NARG(a, b, c, d) == 4);
  static_assert(FURY_PP_NARG(a, b, c, d, e) == 5);
  static_assert(FURY_PP_NARG(x, x, x, x, x, x, x, x, x, x) == 10);
  static_assert(FURY_PP_NARG(x, x, x, x, x, x, x, x, x, x, x, x, x, x, x, x, x,
                             x, x, x, x, x, x, x, x, x, x, x, x, x, x, x, x, x,
                             x, x, x, x, x, x, x, x, x, x, x, x, x, x, x, x, x,
                             x, x, x, x, x, x, x, x, x) == 60);
  static_assert(FURY_PP_NARG(x, x, x, x, x, x, x, x, x, x, x, x, x, x, x, x, x,
                             x, x, x, x, x, x, x, x, x, x, x, x, x, x, x, x, x,
                             x, x, x, x, x, x, x, x, x, x, x, x, x, x, x, x, x,
                             x, x, x, x, x, x, x, x, x, x, x, x) == 63);
}

TEST(Preprocessor, ForEach) {
#define PLUS(n) x += n;
  static_assert([] {
    int x = 0;
    FURY_PP_FOREACH(PLUS, 1);
    FURY_PP_FOREACH(PLUS, 2, 3);
    FURY_PP_FOREACH(PLUS, 4, 5, 6);
    FURY_PP_FOREACH(PLUS, 7, 8, 9, 10);
    FURY_PP_FOREACH(PLUS, 11, 12, 13, 14, 15);
    return x;
  }() == 120);

#define STR(x) #x,
  constexpr std::string_view strings[] = {FURY_PP_FOREACH(STR, a, bc, def)};
  static_assert(strings[0] == "a");
  static_assert(strings[1] == "bc");
  static_assert(strings[2] == "def");
}

} // namespace fury

int main(int argc, char **argv) {
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
