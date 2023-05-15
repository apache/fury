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

import static io.fury.serializer.CompatibleSerializerTest.loadClass;

import io.fury.test.bean.BeanA;
import io.fury.test.bean.CollectionFields;
import io.fury.test.bean.MapFields;

public class ClassUtils {
  public static Class<?> createCompatibleClass1() {
    String pkg = BeanA.class.getPackage().getName();
    String code =
        ""
            + "package "
            + pkg
            + ";\n"
            + "import java.util.*;\n"
            + "import java.math.*;\n"
            + "public class BeanA {\n"
            + "  private Float f4;\n"
            + "  private double f5;\n"
            + "  private BeanB beanB;\n"
            + "  private BeanB beanB_added;\n"
            + "  private int[] intArray;\n"
            + "  private int[] intArray_added;\n"
            + "  private byte[] bytes;\n"
            + "  private transient BeanB f13;\n"
            + "  public BigDecimal f16;\n"
            + "  public String f17;\n"
            + "  public String longStringNameField_added;\n"
            + "  private List<Double> doubleList;\n"
            + "  private Iterable<BeanB> beanBIterable;\n"
            + "  private List<BeanB> beanBList;\n"
            + "  private List<BeanB> beanBList_added;\n"
            + "  private Map<String, BeanB> stringBeanBMap;\n"
            + "  private Map<String, String> stringStringMap_added;\n"
            + "  private int[][] int2DArray;\n"
            + "  private int[][] int2DArray_added;\n"
            + "}";
    return loadClass(BeanA.class, code);
  }

  public static Class<?> createCompatibleClass2() {
    String pkg = CollectionFields.class.getPackage().getName();
    String code =
        ""
            + "package "
            + pkg
            + ";\n"
            + "import java.util.*;\n"
            + "public class CollectionFields {\n"
            + "  public Collection<Integer> collection2;\n"
            + "  public List<Integer> collection3;\n"
            + "  public Collection<String> randomAccessList2;\n"
            + "  public List<String> randomAccessList3;\n"
            + "  public Collection list;\n"
            + "  public Collection<String> list2;\n"
            + "  public List<String> list3;\n"
            + "  public Collection<String> set2;\n"
            + "  public Set<String> set3;\n"
            + "  public Collection<String> sortedSet2;\n"
            + "  public SortedSet<String> sortedSet3;\n"
            + "  public Map map;\n"
            + "  public Map<String, String> map2;\n"
            + "  public SortedMap<Integer, Integer> sortedMap3;"
            + "}";
    return loadClass(CollectionFields.class, code);
  }

  public static Class<?> createCompatibleClass3() {
    String pkg = MapFields.class.getPackage().getName();
    String code =
        ""
            + "package "
            + pkg
            + ";\n"
            + "import java.util.*;\n"
            + "import java.util.concurrent.*;\n"
            + "public class MapFields {\n"
            + " public Map map;\n"
            + "  public Map<String, Integer> map2;\n"
            + "  public Map<String, Integer> map3;\n"
            + "  public Map linkedHashMap;\n"
            + "  public LinkedHashMap<String, Integer> linkedHashMap3;\n"
            + "  public LinkedHashMap<String, Integer> linkedHashMap4;\n"
            + "  public SortedMap sortedMap;\n"
            + "  public SortedMap<String, Integer> sortedMap2;\n"
            + "  public Map concurrentHashMap;\n"
            + "  public ConcurrentHashMap<String, Integer> concurrentHashMap2;\n"
            + "  public ConcurrentSkipListMap skipListMap2;\n"
            + "  public ConcurrentSkipListMap<String, Integer> skipListMap3;\n"
            + "  public ConcurrentSkipListMap<String, Integer> skipListMap4;\n"
            + "  public EnumMap enumMap2;\n"
            + "  public Map emptyMap;\n"
            + "  public Map singletonMap;\n"
            + "  public Map<String, Integer> singletonMap2;\n"
            + "}";
    return loadClass(MapFields.class, code);
  }
}
