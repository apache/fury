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

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertTrue;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;
import io.fury.Fury;
import io.fury.FuryTestBase;
import io.fury.Language;
import io.fury.memory.MemoryBuffer;
import io.fury.memory.MemoryUtils;
import io.fury.test.bean.Cyclic;
import io.fury.test.bean.Struct;
import java.util.ArrayList;
import java.util.List;
import lombok.Data;
import lombok.EqualsAndHashCode;
import lombok.ToString;
import org.testng.annotations.Test;

public class CodegenSerializerTest extends FuryTestBase {

  @Data
  public static class A {}

  @Data
  public static class B {
    public Object f1;
    public Object f2;
    public String f3;
  }

  @Test
  public void testSimpleBean() {
    Fury fury = Fury.builder().withLanguage(Language.JAVA).disableSecureMode().build();
    // serDe(fury, new A());
    B b = new B();
    b.f1 = "str1";
    b.f2 = 1;
    b.f3 = "str3";
    serDe(fury, b);
  }

  @Test
  public void testSupport() {
    assertTrue(CodegenSerializer.supportCodegenForJavaSerialization(Cyclic.class));
  }

  @Test
  public void testSerializeCircularReference() {
    Cyclic cyclic = Cyclic.create(true);
    Fury fury =
        Fury.builder()
            .withLanguage(Language.JAVA)
            .withReferenceTracking(true)
            .disableSecureMode()
            .build();
    MemoryBuffer buffer = MemoryUtils.buffer(32);

    fury.serialize(buffer, cyclic);
    fury.deserialize(buffer);

    Serializer<Cyclic> beanSerializer = fury.getClassResolver().getSerializer(Cyclic.class);
    fury.getReferenceResolver().writeReferenceOrNull(buffer, cyclic);
    beanSerializer.write(buffer, cyclic);
    fury.getReferenceResolver().readReferenceOrNull(buffer);
    fury.getReferenceResolver().preserveReferenceId();
    Cyclic cyclic1 = beanSerializer.read(buffer);
    fury.reset();
    assertEquals(cyclic1, cyclic);
  }

  @Data
  public static class NonFinalPublic {}

  @ToString
  @EqualsAndHashCode(callSuper = true)
  static class NonFinalPackage extends NonFinalPublic {}

  @ToString
  @EqualsAndHashCode(callSuper = true)
  private static class NonFinalPrivate extends NonFinalPublic {}

  public static class TestCacheNonFinalClassInfo {
    public String str;
    public NonFinalPublic nonFinalPublic;
    public List<String> finalList;
    public List<NonFinalPublic> nonFinalPublicList;
    public List<NonFinalPackage> packageList;
    public List<NonFinalPrivate> nonFinalPrivateList;
  }

  @Test
  public void testCacheNonFinalClassInfo() {
    Fury fury = Fury.builder().withLanguage(Language.JAVA).disableSecureMode().build();
    TestCacheNonFinalClassInfo obj = new TestCacheNonFinalClassInfo();
    obj.finalList = new ArrayList<>(ImmutableList.of("a", "b"));
    obj.nonFinalPublicList =
        new ArrayList<>(
            ImmutableList.of(new NonFinalPublic(), new NonFinalPublic(), new NonFinalPrivate()));
    obj.packageList = new ArrayList<>(ImmutableList.of(new NonFinalPackage()));
    obj.nonFinalPrivateList = new ArrayList<>(ImmutableList.of(new NonFinalPrivate()));
    serDe(fury, obj);
  }

  @Test(dataProvider = "compressNumber")
  public void testCompressInt(boolean compressNumber) {
    Fury fury =
        Fury.builder()
            .withLanguage(Language.JAVA)
            .withNumberCompressed(compressNumber)
            .withCompatibleMode(CompatibleMode.SCHEMA_CONSISTENT)
            .withClassRegistrationRequired(false)
            .build();
    Class<?> structClass = Struct.createNumberStructClass("CompressInt", 50);
    serDeCheck(fury, Struct.createPOJO(structClass));
  }

  @Data
  public static final class Column {
    byte[] family;
    byte[] column;
    Object value;

    public Column(byte[] family, byte[] column, Object value) {
      this.family = family;
      this.column = column;
      this.value = value;
    }
  }

  @Data
  public static class Get {
    List<Column> pkColumns;

    public Get(List<Column> pkColumns) {
      this.pkColumns = pkColumns;
    }
  }

  @Test
  public void testFinalTypeField() {
    Fury fury =
        Fury.builder()
            .withLanguage(Language.JAVA)
            .withReferenceTracking(false)
            .withClassRegistrationRequired(true)
            .withCompatibleMode(CompatibleMode.SCHEMA_CONSISTENT)
            .build();
    fury.register(Column.class);
    fury.register(Get.class);
    Get get = new Get(Lists.newArrayList(new Column(new byte[] {1}, new byte[] {2}, "abc")));
    serDeCheck(fury, get);
  }
}
