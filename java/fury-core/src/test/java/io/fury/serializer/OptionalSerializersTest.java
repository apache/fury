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

package io.fury.serializer;

import io.fury.Fury;
import io.fury.FuryTestBase;
import io.fury.Language;
import java.util.Optional;
import java.util.OptionalDouble;
import java.util.OptionalInt;
import java.util.OptionalLong;
import org.testng.annotations.Test;

public class OptionalSerializersTest extends FuryTestBase {

  @Test
  void testOptionals() {
    Fury fury = Fury.builder().withLanguage(Language.JAVA).requireClassRegistration(false).build();
    serDeCheckSerializerAndEqual(fury, Optional.empty(), "Optional");
    serDeCheckSerializerAndEqual(fury, Optional.of("abc"), "Optional");
    serDeCheckSerializerAndEqual(fury, OptionalInt.empty(), "Optional");
    serDeCheckSerializerAndEqual(fury, OptionalInt.of(Integer.MIN_VALUE), "Optional");
    serDeCheckSerializerAndEqual(fury, OptionalInt.of(Integer.MAX_VALUE), "Optional");
    serDeCheckSerializerAndEqual(fury, OptionalLong.empty(), "Optional");
    serDeCheckSerializerAndEqual(fury, OptionalLong.of(Long.MIN_VALUE), "Optional");
    serDeCheckSerializerAndEqual(fury, OptionalLong.of(Long.MAX_VALUE), "Optional");
    serDeCheckSerializerAndEqual(fury, OptionalDouble.empty(), "Optional");
    serDeCheckSerializerAndEqual(fury, OptionalDouble.of(Double.MIN_VALUE), "Optional");
    serDeCheckSerializerAndEqual(fury, OptionalDouble.of(Double.MAX_VALUE), "Optional");
  }
}
