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

package org.apache.fury.type;

import static org.testng.Assert.assertEquals;

import com.google.common.collect.ImmutableList;
import java.lang.reflect.Type;
import java.util.AbstractList;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import org.apache.fury.collection.Tuple2;
import org.apache.fury.reflect.TypeToken;
import org.apache.fury.test.bean.BeanA;
import org.apache.fury.test.bean.BeanB;
import org.testng.Assert;
import org.testng.annotations.Test;

@SuppressWarnings({"rawtypes", "UnstableApiUsage"})
public class TypeUtilsTest {
  @Test
  public void getArrayTypeTest() {
    assertEquals("int[][][][][]", TypeUtils.getArrayType(int[][][][][].class));
    assertEquals("java.lang.Object[][][][][]", TypeUtils.getArrayType(Object[][][][][].class));
    assertEquals("int[][][][][]", TypeUtils.getArrayType(TypeToken.of(int[][][][][].class)));
    assertEquals(
        "java.lang.Object[][][][][]", TypeUtils.getArrayType(TypeToken.of(Object[][][][][].class)));
  }

  @Test
  public void getElementTypeTest() throws NoSuchMethodException {
    TypeToken typeToken =
        Descriptor.getDescriptorsMap(BeanA.class).get("doubleList").getTypeToken();

    @SuppressWarnings("unchecked")
    TypeToken<?> supertype =
        ((TypeToken<? extends Iterable<?>>) typeToken).getSupertype(Iterable.class);
    final Type iteratorReturnType = Iterable.class.getMethod("iterator").getGenericReturnType();
    final Type nextReturnType = Iterator.class.getMethod("next").getGenericReturnType();
    assertEquals(supertype.resolveType(iteratorReturnType), new TypeToken<Iterator<Double>>() {});
    assertEquals(
        supertype.resolveType(iteratorReturnType).resolveType(nextReturnType),
        new TypeToken<Double>() {});
  }

  @Test
  public void getSubclassElementTypeTest() {
    abstract class A implements Collection<List<String>> {}
    Assert.assertEquals(
        TypeUtils.getElementType(TypeToken.of(A.class)), new TypeToken<List<String>>() {});
  }

  @Test
  public void getMapKeyValueTypeTest() throws NoSuchMethodException {
    TypeToken typeToken =
        Descriptor.getDescriptorsMap(BeanA.class).get("stringBeanBMap").getTypeToken();

    @SuppressWarnings("unchecked")
    TypeToken<?> supertype = ((TypeToken<? extends Map<?, ?>>) typeToken).getSupertype(Map.class);

    final Type keySetReturnType = Map.class.getMethod("keySet").getGenericReturnType();
    final Type valuesReturnType = Map.class.getMethod("values").getGenericReturnType();
    TypeToken<?> keyType = TypeUtils.getElementType(supertype.resolveType(keySetReturnType));
    assertEquals(TypeToken.of(String.class), keyType);
    TypeToken<?> valueType = TypeUtils.getElementType(supertype.resolveType(valuesReturnType));
    assertEquals(TypeToken.of(BeanB.class), valueType);
    Tuple2<TypeToken<?>, TypeToken<?>> mapKeyValueType =
        TypeUtils.getMapKeyValueType(TypeToken.of(Map.class));
    System.out.println(mapKeyValueType);
  }

  public static class Cyclic {
    public Cyclic list;
    public Bean bean;
  }

  public static class Bean {
    public ArrayList list;
    public AbstractList abstractList;
  }

  @Test
  public void isBeanTest() {
    Assert.assertTrue(TypeUtils.isBean(BeanA.class));
    Assert.assertFalse(TypeUtils.isBean(Object.class));
  }

  @Test
  public void listBeansRecursiveInclusiveTest() {
    LinkedHashSet<Class<?>> classes = TypeUtils.listBeansRecursiveInclusive(BeanA.class);
    // System.out.println(classes);
    assertEquals(classes.size(), 2);
  }

  @Test(expectedExceptions = UnsupportedOperationException.class)
  public void listBeansCyclic() {
    LinkedHashSet<Class<?>> classes = TypeUtils.listBeansRecursiveInclusive(Cyclic.class);
    // System.out.println(classes);
    assertEquals(classes.size(), 2);
  }

  @Test
  public void isBean() {
    Assert.assertTrue(TypeUtils.isBean(BeanA.class));
    Assert.assertTrue(TypeUtils.isBean(Bean.class));
    Assert.assertFalse(TypeUtils.isBean(ArrayList.class));
  }

  @Test
  public void isSupported() {
    TypeUtils.isSupported(TypeToken.of(AbstractList.class));
  }

  public static class A {
    @Override
    public String toString() {
      return "A{}";
    }
  }

  @Test
  public void checkMethodOverride() throws NoSuchMethodException {
    assertEquals(A.class.getMethod("toString").getDeclaringClass(), A.class);
    assertEquals(A.class.getMethod("hashCode").getDeclaringClass(), Object.class);
  }

  @Test
  public void testTypeConstruct() {
    assertEquals(TypeUtils.arrayListOf(Integer.class), new TypeToken<ArrayList<Integer>>() {});
    assertEquals(TypeUtils.listOf(Integer.class), new TypeToken<List<Integer>>() {});
    assertEquals(
        TypeUtils.mapOf(String.class, Integer.class), new TypeToken<Map<String, Integer>>() {});
    assertEquals(
        TypeUtils.hashMapOf(String.class, Integer.class),
        new TypeToken<HashMap<String, Integer>>() {});
  }

  @Test
  public void testMapOf() {
    TypeToken<Map<String, Integer>> mapTypeToken = TypeUtils.mapOf(String.class, Integer.class);
    Assert.assertEquals(mapTypeToken, new TypeToken<Map<String, Integer>>() {});
    Assert.assertEquals(
        TypeUtils.mapOf(HashMap.class, String.class, Integer.class),
        new TypeToken<HashMap<String, Integer>>() {});
    Assert.assertEquals(
        TypeUtils.hashMapOf(String.class, Integer.class),
        new TypeToken<HashMap<String, Integer>>() {});
    Assert.assertEquals(
        TypeUtils.mapOf(LinkedHashMap.class, String.class, Integer.class),
        new TypeToken<LinkedHashMap<String, Integer>>() {});
  }

  @Test
  public void testMaxType() {
    assertEquals(TypeUtils.maxType(int.class, long.class), long.class);
    assertEquals(TypeUtils.maxType(long.class, int.class), long.class);
    assertEquals(TypeUtils.maxType(float.class, long.class), long.class);
    assertEquals(TypeUtils.maxType(long.class, float.class), long.class);
    List<Class<?>> classes =
        Arrays.asList(
            void.class,
            boolean.class,
            byte.class,
            char.class,
            short.class,
            int.class,
            float.class,
            long.class,
            double.class);
    for (int i = 0; i < classes.size() - 1; i++) {
      assertEquals(TypeUtils.maxType(classes.get(i), classes.get(i + 1)), classes.get(i + 1));
    }
  }

  @Test
  public void testGetSizeOfPrimitiveType() {
    List<Integer> sizes =
        ImmutableList.of(
                void.class,
                boolean.class,
                byte.class,
                char.class,
                short.class,
                int.class,
                float.class,
                long.class,
                double.class)
            .stream()
            .map(TypeUtils::getSizeOfPrimitiveType)
            .collect(Collectors.toList());
    assertEquals(sizes, ImmutableList.of(0, 1, 1, 2, 2, 4, 4, 8, 8));
  }

  public static class Test3<T1 extends Number & Comparable, T2 extends Map> {
    public ArrayList<String> fromField3;
    public List raw;
    public T2 unknown2;
    public T2[] arrayUnknown2;
    public ArrayList<?> unboundWildcard;
    public ArrayList<? extends Number> upperBound;
  }

  @Test
  public void testGetRawType() throws NoSuchFieldException {
    assertEquals(
        TypeUtils.getRawType(Test3.class.getDeclaredField("fromField3").getGenericType()),
        ArrayList.class);
    assertEquals(
        TypeUtils.getRawType(Test3.class.getDeclaredField("raw").getGenericType()), List.class);
    assertEquals(
        TypeUtils.getRawType(Test3.class.getDeclaredField("unknown2").getGenericType()), Map.class);
    assertEquals(
        TypeUtils.getRawType(Test3.class.getDeclaredField("arrayUnknown2").getGenericType()),
        Map[].class);
    assertEquals(
        TypeUtils.getRawType(Test3.class.getDeclaredField("unboundWildcard").getGenericType()),
        ArrayList.class);
    assertEquals(
        TypeUtils.getRawType(Test3.class.getDeclaredField("upperBound").getGenericType()),
        ArrayList.class);
  }

  @Test
  public void getTypeArguments() {
    TypeToken<Tuple2<String, Map<String, Integer>>> typeToken =
        new TypeToken<Tuple2<String, Map<String, Integer>>>() {};
    assertEquals(TypeUtils.getTypeArguments(typeToken).size(), 2);
  }

  @Test
  public void getAllTypeArguments() {
    TypeToken<Tuple2<String, Map<String, BeanA>>> typeToken =
        new TypeToken<Tuple2<String, Map<String, BeanA>>>() {};
    List<TypeToken<?>> allTypeArguments = TypeUtils.getAllTypeArguments(typeToken);
    assertEquals(allTypeArguments.size(), 3);
    assertEquals(allTypeArguments.get(2).getRawType(), BeanA.class);
  }
}
