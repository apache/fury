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

package org.apache.fory.resolver;

import static org.apache.fory.resolver.FieldResolver.FieldInfo;
import static org.apache.fory.resolver.FieldResolver.computeStringHash;
import static org.apache.fory.resolver.FieldResolver.decodeLongAsString;
import static org.apache.fory.resolver.FieldResolver.encodeFieldNameAsLong;
import static org.apache.fory.resolver.FieldResolver.encodingBytesLength;
import static org.apache.fory.resolver.FieldResolver.of;

import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import org.apache.fory.Fory;
import org.apache.fory.config.Language;
import org.testng.Assert;
import org.testng.annotations.Test;

public class FieldResolverTest {

  @Test
  public void testEncodingBytesLength() {
    Assert.assertEquals(encodingBytesLength("abc"), 3);
    Assert.assertEquals(encodingBytesLength("abcd12345"), 7);
    Assert.assertEquals(encodingBytesLength("abcd1234abcd"), 9);
  }

  @Test
  public void testEncodeFieldNameAsLong() {
    Assert.assertEquals(decodeLongAsString(encodeFieldNameAsLong("123"), 18), "123");
    Assert.assertEquals(decodeLongAsString(encodeFieldNameAsLong("abc"), 18), "abc");
    Assert.assertEquals(decodeLongAsString(encodeFieldNameAsLong("ABC"), 18), "ABC");
    Assert.assertEquals(decodeLongAsString(encodeFieldNameAsLong("abcd145"), 42), "abcd145");
    Assert.assertEquals(decodeLongAsString(encodeFieldNameAsLong("Abcd145"), 42), "Abcd145");
    Assert.assertEquals(decodeLongAsString(encodeFieldNameAsLong("abcd12345"), 54), "abcd12345");
    Assert.assertEquals(decodeLongAsString(encodeFieldNameAsLong("aBcD12Z4z"), 54), "aBcD12Z4z");
  }

  private static final class EmbeddedClassIdTestClass {
    private int f1;
    private Integer intField;
    private String longFieldNameString;
    private List<String> longFieldNameList;
  }

  @Test
  public void testEmbeddedClassId() {
    Fory fory = Fory.builder().withLanguage(Language.JAVA).build();
    FieldResolver fieldResolver = of(fory, EmbeddedClassIdTestClass.class);
    FieldInfo f1Field = fieldResolver.getEmbedTypes4Fields()[0];
    Short classId = (short) ((f1Field.getEncodedFieldInfo() & 0xff) >> 2);
    Assert.assertEquals(
        classId, fory.getClassResolver().getRegisteredClassId(f1Field.getField().getType()));
    FieldInfo intField = fieldResolver.getEmbedTypes9Fields()[0];
    classId = (short) ((intField.getEncodedFieldInfo() & 0b1111111111) >>> 3);
    Assert.assertEquals(
        classId, fory.getClassResolver().getRegisteredClassId(intField.getField().getType()));
    FieldInfo longFieldNameField = fieldResolver.getEmbedTypesHashFields()[0];
    classId = (short) ((longFieldNameField.getEncodedFieldInfo() & 0b1111111111) >>> 3);
    Assert.assertEquals(
        classId,
        fory.getClassResolver().getRegisteredClassId(longFieldNameField.getField().getType()));
  }

  private static final class FieldsSortedTestClass {
    private int bc12;
    private int abc;
    private int a12;
    private int BCD12;
    private int ACD12;
    private int abcd123;
    private int ace12345;
    private int cde12345678;
    private int bcd12345678;
    private int ABC12345678;
    private List<String> longFieldNameList;
    private Set<String> longFieldNameSet;
  }

  @Test
  public void testFieldsSorted() {
    Fory fory = Fory.builder().withLanguage(Language.JAVA).build();
    FieldResolver fieldResolver = of(fory, FieldsSortedTestClass.class);
    FieldInfo[] embedTypes4Fields = fieldResolver.getEmbedTypes4Fields();
    Assert.assertEquals(embedTypes4Fields.length, 3);
    Assert.assertEquals(embedTypes4Fields[0].getField().getName(), "bc12"); // max bit is used.
    Assert.assertEquals(embedTypes4Fields[1].getField().getName(), "a12");
    Assert.assertEquals(embedTypes4Fields[2].getField().getName(), "abc");
    FieldInfo[] embedTypes9Fields = fieldResolver.getEmbedTypes9Fields();
    Assert.assertEquals(embedTypes9Fields.length, 4);
    Assert.assertEquals(embedTypes9Fields[0].getField().getName(), "ACD12");
    Assert.assertEquals(embedTypes9Fields[1].getField().getName(), "BCD12");
    Assert.assertEquals(embedTypes9Fields[2].getField().getName(), "abcd123");
    Assert.assertEquals(embedTypes9Fields[3].getField().getName(), "ace12345");
    FieldInfo[] embedTypesHashFields = fieldResolver.getEmbedTypesHashFields();
    TreeMap<Long, String> hash2FieldsMap = new TreeMap<>(Long::compareTo);
    Assert.assertEquals(embedTypesHashFields.length, 3);
    hash2FieldsMap.put(computeStringHash("ABC12345678"), "ABC12345678");
    hash2FieldsMap.put(computeStringHash("cde12345678"), "cde12345678");
    hash2FieldsMap.put(computeStringHash("bcd12345678"), "bcd12345678");
    int idx = 0;
    for (Map.Entry<Long, String> longStringEntry : hash2FieldsMap.entrySet()) {
      Assert.assertEquals(
          embedTypesHashFields[idx++].getField().getName(), longStringEntry.getValue());
    }
    FieldInfo[] separateTypesHashFields = fieldResolver.getSeparateTypesHashFields();
    Assert.assertEquals(separateTypesHashFields.length, 2);
    hash2FieldsMap.clear();
    hash2FieldsMap.put(computeStringHash("longFieldNameList") << 2, "longFieldNameList");
    hash2FieldsMap.put(computeStringHash("longFieldNameSet") << 2, "longFieldNameSet");
    idx = 0;
    for (Map.Entry<Long, String> longStringEntry : hash2FieldsMap.entrySet()) {
      Assert.assertEquals(
          separateTypesHashFields[idx++].getField().getName(), longStringEntry.getValue());
    }
  }
}
