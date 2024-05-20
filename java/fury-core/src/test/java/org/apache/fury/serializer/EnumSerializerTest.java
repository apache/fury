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

package org.apache.fury.serializer;

import static org.testng.Assert.*;

import org.apache.fury.Fury;
import org.apache.fury.FuryTestBase;
import org.apache.fury.codegen.JaninoUtils;
import org.apache.fury.config.FuryBuilder;
import org.apache.fury.config.Language;
import org.testng.annotations.Test;

public class EnumSerializerTest extends FuryTestBase {

  @Test
  public void testWrite() {}

  public enum EnumFoo {
    A,
    B
  }

  public enum EnumSubClass {
    A {
      @Override
      void f() {}
    },
    B {
      @Override
      void f() {}
    };

    abstract void f();
  }

  @Test(dataProvider = "crossLanguageReferenceTrackingConfig")
  public void testEnumSerialization(boolean referenceTracking, Language language) {
    FuryBuilder builder =
        Fury.builder()
            .withLanguage(language)
            .withRefTracking(referenceTracking)
            .requireClassRegistration(false);
    Fury fury1 = builder.build();
    Fury fury2 = builder.build();
    assertEquals(EnumSerializerTest.EnumFoo.A, serDe(fury1, fury2, EnumSerializerTest.EnumFoo.A));
    assertEquals(EnumSerializerTest.EnumFoo.B, serDe(fury1, fury2, EnumSerializerTest.EnumFoo.B));
    assertEquals(
        EnumSerializerTest.EnumSubClass.A, serDe(fury1, fury2, EnumSerializerTest.EnumSubClass.A));
    assertEquals(
        EnumSerializerTest.EnumSubClass.B, serDe(fury1, fury2, EnumSerializerTest.EnumSubClass.B));
  }

  @Test()
  public void testEnumSerializationUnexistentEnumValueAsNull() {
    String enumCode2 = "enum TestEnum2 {" + " A;" + "}";
    String enumCode1 = "enum TestEnum2 {" + " A, B" + "}";
    Class<?> cls1 =
        JaninoUtils.compileClass(getClass().getClassLoader(), "", "TestEnum2", enumCode1);
    Class<?> cls2 =
        JaninoUtils.compileClass(getClass().getClassLoader(), "", "TestEnum2", enumCode2);
    FuryBuilder builderSerialization =
        Fury.builder()
            .withLanguage(Language.JAVA)
            .withRefTracking(true)
            .requireClassRegistration(false);
    FuryBuilder builderDeserialize =
        Fury.builder()
            .withLanguage(Language.JAVA)
            .withRefTracking(true)
            .requireClassRegistration(false)
            .deserializeNonexistentEnumValueAsNull(true)
            .withClassLoader(cls2.getClassLoader());
    Fury furyDeserialize = builderDeserialize.build();
    Fury furySerialization = builderSerialization.build();
    byte[] bytes = furySerialization.serialize(cls1.getEnumConstants()[1]);
    Object data = furyDeserialize.deserialize(bytes);
  }
}
