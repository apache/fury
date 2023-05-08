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

package io.fury.serializer;

import io.fury.Fury;
import io.fury.FuryTestBase;
import io.fury.Language;
import org.testng.annotations.Test;

public class ArraySerializersTest extends FuryTestBase {
  @Test(dataProvider = "crossLanguageReferenceTrackingConfig")
  public void testObjectArraySerialization(boolean referenceTracking, Language language) {
    Fury.FuryBuilder builder =
        Fury.builder()
            .withLanguage(language)
            .withReferenceTracking(referenceTracking)
            .disableSecureMode();
    Fury fury1 = builder.build();
    Fury fury2 = builder.build();
    serDeCheck(fury1, fury2, new Object[] {false, true});
    serDeCheck(fury1, fury2, new Object[] {(byte) 1, (byte) 1});
    serDeCheck(fury1, fury2, new Object[] {(short) 1, (short) 1});
    serDeCheck(fury1, fury2, new Object[] {(char) 1, (char) 1});
    serDeCheck(fury1, fury2, new Object[] {1, 1});
    serDeCheck(fury1, fury2, new Object[] {(float) 1.0, (float) 1.1});
    serDeCheck(fury1, fury2, new Object[] {1.0, 1.1});
    serDeCheck(fury1, fury2, new Object[] {1L, 2L});
    serDeCheck(fury1, fury2, new Boolean[] {false, true});
    serDeCheck(fury1, fury2, new Byte[] {(byte) 1, (byte) 1});
    serDeCheck(fury1, fury2, new Short[] {(short) 1, (short) 1});
    serDeCheck(fury1, fury2, new Character[] {(char) 1, (char) 1});
    serDeCheck(fury1, fury2, new Integer[] {1, 1});
    serDeCheck(fury1, fury2, new Float[] {(float) 1.0, (float) 1.1});
    serDeCheck(fury1, fury2, new Double[] {1.0, 1.1});
    serDeCheck(fury1, fury2, new Long[] {1L, 2L});
    serDeCheck(
        fury1, fury2, new Object[] {false, true, (byte) 1, (byte) 1, (float) 1.0, (float) 1.1});
  }

  @Test(dataProvider = "crossLanguageReferenceTrackingConfig")
  public void testMultiArraySerialization(boolean referenceTracking, Language language) {
    Fury.FuryBuilder builder =
        Fury.builder()
            .withLanguage(language)
            .withReferenceTracking(referenceTracking)
            .disableSecureMode();
    Fury fury1 = builder.build();
    Fury fury2 = builder.build();
    serDeCheck(fury1, fury2, new Object[][] {{false, true}, {false, true}});
    serDeCheck(
        fury1,
        fury2,
        new Object[][] {
          {false, true, (byte) 1, (byte) 1, (float) 1.0, (float) 1.1},
          {false, true, (byte) 1, (byte) 1, (float) 1.0, (float) 1.1}
        });
  }
}
