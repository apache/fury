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

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertTrue;

import java.lang.reflect.Array;
import java.lang.reflect.Field;
import java.util.Arrays;
import java.util.List;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;
import lombok.Data;
import lombok.EqualsAndHashCode;
import org.apache.fury.Fury;
import org.apache.fury.FuryTestBase;
import org.apache.fury.config.FuryBuilder;
import org.apache.fury.config.Language;
import org.apache.fury.reflect.ReflectionUtils;
import org.apache.fury.test.bean.ArraysData;
import org.apache.fury.type.Descriptor;
import org.testng.Assert;
import org.testng.annotations.Test;

public class ArraySerializersTest extends FuryTestBase {

  @Test(dataProvider = "crossLanguageReferenceTrackingConfig")
  public void testObjectArraySerialization(boolean referenceTracking, Language language) {
    FuryBuilder builder =
        Fury.builder()
            .withLanguage(language)
            .withRefTracking(referenceTracking)
            .requireClassRegistration(false);
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
    serDeCheck(fury1, fury2, new String[] {"str", "str"});
    serDeCheck(fury1, fury2, new Object[] {"str", 1});
  }

  @Test(dataProvider = "crossLanguageReferenceTrackingConfig")
  public void testMultiArraySerialization(boolean referenceTracking, Language language) {
    FuryBuilder builder =
        Fury.builder()
            .withLanguage(language)
            .withRefTracking(referenceTracking)
            .requireClassRegistration(false);
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
    serDeCheck(fury1, fury2, new Integer[][] {{1, 2}, {1, 2}});
  }

  @Test(dataProvider = "crossLanguageReferenceTrackingConfig")
  public void testPrimitiveArray(boolean referenceTracking, Language language) {
    FuryBuilder builder =
        Fury.builder()
            .withLanguage(language)
            .withRefTracking(referenceTracking)
            .requireClassRegistration(false);
    Fury fury1 = builder.build();
    Fury fury2 = builder.build();
    testPrimitiveArray(fury1, fury2);
  }

  public static void testPrimitiveArray(Fury fury1, Fury fury2) {
    assertTrue(
        Arrays.equals(
            new boolean[] {false, true},
            (boolean[]) serDe(fury1, fury2, new boolean[] {false, true})));
    assertEquals(new byte[] {1, 1}, (byte[]) serDe(fury1, fury2, new byte[] {1, 1}));
    assertEquals(new short[] {1, 1}, (short[]) serDe(fury1, fury2, new short[] {1, 1}));
    assertEquals(new int[] {1, 1}, (int[]) serDe(fury1, fury2, new int[] {1, 1}));
    assertEquals(new long[] {1, 1}, (long[]) serDe(fury1, fury2, new long[] {1, 1}));
    assertTrue(
        Arrays.equals(new float[] {1.f, 1.f}, (float[]) serDe(fury1, fury2, new float[] {1f, 1f})));
    assertTrue(
        Arrays.equals(
            new double[] {1.0, 1.0}, (double[]) serDe(fury1, fury2, new double[] {1.0, 1.0})));
  }

  @Test(dataProvider = "referenceTrackingConfig")
  public void testArrayZeroCopy(boolean referenceTracking) {
    FuryBuilder builder =
        Fury.builder()
            .withLanguage(Language.JAVA)
            .withRefTracking(referenceTracking)
            .requireClassRegistration(false);
    Fury fury1 = builder.build();
    Fury fury2 = builder.build();
    AtomicInteger counter = new AtomicInteger(0);
    for (int i = 0; i < 4; i++) {
      ArraysData arraysData = new ArraysData(7 * i);
      Set<Field> fields = Descriptor.getFields(ArraysData.class);
      List<Object> fieldValues = ReflectionUtils.getFieldValues(fields, arraysData);
      Object[] array = fieldValues.toArray(new Object[0]);
      assertEquals(array, serDeOutOfBand(counter, fury1, fury1, array));
      assertEquals(array, serDeOutOfBand(counter, fury1, fury2, array));
    }
  }

  @Test(dataProvider = "referenceTrackingConfig")
  public void testArrayStructZeroCopy(boolean referenceTracking) {
    FuryBuilder builder =
        Fury.builder()
            .withLanguage(Language.JAVA)
            .withRefTracking(referenceTracking)
            .requireClassRegistration(false);
    Fury fury1 = builder.build();
    Fury fury2 = builder.build();
    AtomicInteger counter = new AtomicInteger(0);
    for (int i = 0; i < 4; i++) {
      ArraysData arraysData = new ArraysData(7 * i);
      assertEquals(arraysData, serDeOutOfBand(counter, fury1, fury1, arraysData));
      assertEquals(arraysData, serDeOutOfBand(counter, fury1, fury2, arraysData));
    }
  }

  @EqualsAndHashCode
  static class A {
    final int f1;

    A(int f1) {
      this.f1 = f1;
    }
  }

  @EqualsAndHashCode(callSuper = true)
  static class B extends A {
    final String f2;

    B(int f1, String f2) {
      super(f1);
      this.f2 = f2;
    }
  }

  @Data
  static class Struct {
    A[] arr;

    public Struct(A[] arr) {
      this.arr = arr;
    }
  }

  static class GenericArrayWrapper<T> {
    private final T[] array;

    @SuppressWarnings("unchecked")
    public GenericArrayWrapper(Class<T> clazz, int capacity) {
      this.array = (T[]) Array.newInstance(clazz, capacity);
    }
  }

  @SuppressWarnings("unchecked")
  @Test(dataProvider = "enableCodegen")
  public void testArrayPolyMorphic(boolean enableCodegen) {
    Fury fury = Fury.builder().requireClassRegistration(false).withCodegen(enableCodegen).build();
    Object[] arr = new String[] {"a", "b"};
    serDeCheck(fury, arr);

    A[] arr1 = new B[] {new B(1, "a"), new B(2, "b")};
    serDeCheck(fury, arr1);

    Struct struct1 = new Struct(arr1);
    serDeCheck(fury, struct1);
    A[] arr2 = new A[] {new A(1), new B(2, "b")};
    Struct struct2 = new Struct(arr2);
    serDeCheck(fury, struct2);

    final GenericArrayWrapper<String> wrapper = new GenericArrayWrapper<>(String.class, 2);
    wrapper.array[0] = "Hello";
    final byte[] bytes = fury.serialize(wrapper);
    final GenericArrayWrapper<String> deserialized =
        (GenericArrayWrapper<String>) fury.deserialize(bytes);
    deserialized.array[1] = "World";
    Assert.assertEquals(deserialized.array, new String[] {"Hello", "World"});
  }
}
