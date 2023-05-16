/*
 * Copyright 2023 The Fury authors
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package io.fury;

import static org.testng.Assert.assertEquals;

import java.util.StringTokenizer;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

public class FuryTest extends FuryTestBase {
  @DataProvider(name = "languageConfig")
  public static Object[] languageConfig() {
    return new Object[] {Language.JAVA, Language.PYTHON};
  }

  @Test(dataProvider = "crossLanguageReferenceTrackingConfig")
  public void primitivesTest(boolean referenceTracking, Language language) {
    Fury fury1 =
        Fury.builder()
            .withLanguage(language)
            .withReferenceTracking(referenceTracking)
            .disableSecureMode()
            .build();
    Fury fury2 =
        Fury.builder()
            .withLanguage(language)
            .withReferenceTracking(referenceTracking)
            .disableSecureMode()
            .build();
    assertEquals(true, serDe(fury1, fury2, true));
    assertEquals(Byte.MAX_VALUE, serDe(fury1, fury2, Byte.MAX_VALUE));
    assertEquals(Short.MAX_VALUE, serDe(fury1, fury2, Short.MAX_VALUE));
    assertEquals(Integer.MAX_VALUE, serDe(fury1, fury2, Integer.MAX_VALUE));
    assertEquals(Long.MAX_VALUE, serDe(fury1, fury2, Long.MAX_VALUE));
    assertEquals(Float.MAX_VALUE, serDe(fury1, fury2, Float.MAX_VALUE));
    assertEquals(Double.MAX_VALUE, serDe(fury1, fury2, Double.MAX_VALUE));
  }

  @Test(dataProvider = "enableCodegen")
  public void testSerializeJDKObject(boolean enableCodegen) {
    Fury fury =
        Fury.builder()
            .withLanguage(Language.JAVA)
            .withJdkClassSerializableCheck(false)
            .disableSecureMode()
            .withCodegen(enableCodegen)
            .build();
    StringTokenizer tokenizer = new StringTokenizer("abc,1,23", ",");
    assertEquals(serDe(fury, tokenizer).countTokens(), tokenizer.countTokens());
  }
}
