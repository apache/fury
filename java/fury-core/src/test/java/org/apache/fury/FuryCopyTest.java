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

package org.apache.fury;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.net.URI;
import java.nio.charset.Charset;
import java.sql.Time;
import java.sql.Timestamp;
import java.time.Duration;
import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.MonthDay;
import java.time.OffsetDateTime;
import java.time.OffsetTime;
import java.time.Period;
import java.time.Year;
import java.time.YearMonth;
import java.time.ZoneOffset;
import java.time.ZonedDateTime;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.BitSet;
import java.util.Collections;
import java.util.Comparator;
import java.util.Currency;
import java.util.EnumMap;
import java.util.EnumSet;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.OptionalDouble;
import java.util.OptionalInt;
import java.util.OptionalLong;
import java.util.PriorityQueue;
import java.util.TreeMap;
import java.util.TreeSet;
import java.util.UUID;
import java.util.Vector;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentSkipListMap;
import java.util.concurrent.ConcurrentSkipListSet;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.regex.Pattern;
import org.apache.fury.collection.LazyMap;
import org.apache.fury.serializer.EnumSerializerTest;
import org.apache.fury.serializer.EnumSerializerTest.EnumFoo;
import org.apache.fury.util.DateTimeUtils;
import org.testng.Assert;
import org.testng.annotations.Test;

public class FuryCopyTest extends FuryTestBase {

  private final Fury fury = builder().withCodegen(false).build();

  @Test
  public void circularRefCopyTest() {
    A a = new A();
    B b = new B();
    a.setName("a");
    b.setName("b");
    a.setB(b);
    b.setA(a);
    A aa = fury.copy(a);
    B bb = fury.copy(b);

    System.out.println(a);
    System.out.println(aa);
    System.out.println(b);
    System.out.println(bb);

    a.list.add(1);
    b.list.add(2);
    System.out.println(a);
    System.out.println(aa);
    System.out.println(b);
    System.out.println(bb);
  }

  @Test
  public void immutableObjectCopyTest() {
    primitiveCopyTest();
    assertSame("12ca213@!.3");
    assertSame(EnumSerializerTest.EnumFoo.A);
    assertSame(EnumSerializerTest.EnumFoo.B);
    assertSame(BigInteger.valueOf(100));
    assertSame(BigDecimal.valueOf(100, 2));
    timeCopyTest();
    assertSame(Charset.defaultCharset());
    assertSame(Object.class);
    assertSame(Currency.getInstance(Locale.CHINA));
    assertSame(new Object());
    assertSame(Locale.US);
    assertSame(OptionalInt.of(Integer.MAX_VALUE));
    assertSame(OptionalDouble.of(Double.MAX_VALUE));
    assertSame(OptionalLong.of(Long.MAX_VALUE));
    assertSame(Pattern.compile(""));
    assertSame(URI.create("test"));
    assertSame(new UUID(System.currentTimeMillis(), System.currentTimeMillis()));
    // assertSame(new URL("https://www.baidu.com"));   // URL is not handle with URLSerializer. use replaceResolveSerializer

    assertSame(Collections.EMPTY_LIST);
    assertSame(Collections.EMPTY_MAP);
    assertSame(Collections.EMPTY_SET);
    assertSame(Collections.emptySortedSet());
    assertSame(Collections.emptySortedMap());
  }

  @Test
  public void mutableObjectCopyTest() {
    collectionCopyTest();
    mapCopyTest();
    objectCopyTest();
  }

  @Test
  public void threadLocalCopyTest() {
    // todo: theadlocal fury copy test use case
  }

  @Test
  public void threadpoolCopyTest() {
    // todo: threadpool fury copy test use case
  }

  private void objectCopyTest() {
    // todo: complex objects、circular reference copy test use case
  }

  private void mapCopyTest() {
    Map<String, Integer> map = Map.of("a", 1, "b", 2);
    TreeMap<String, Integer> treeMap = new TreeMap<>(Comparator.naturalOrder());
    treeMap.putAll(map);
    EnumMap<EnumFoo, Object> enumMap = new EnumMap<>(EnumFoo.class);
    enumMap.put(EnumFoo.A, 1);
    LazyMap<String, Integer> lazyMap = new LazyMap<>();
    lazyMap.put("a", 1);

    assetEqualsButNotSame(new HashMap<>(map));
    assetEqualsButNotSame(new LinkedHashMap<>(map));
    assetEqualsButNotSame(treeMap);
    assetEqualsButNotSame(Collections.singletonMap("a", 1));
    assetEqualsButNotSame(new ConcurrentHashMap<>(map));
    assetEqualsButNotSame(new ConcurrentSkipListMap<>(map));
    assetEqualsButNotSame(enumMap);
    assetEqualsButNotSame(lazyMap);
  }

  private void collectionCopyTest() {
    List<String> testData = Arrays.asList("1", "2", "3");
    TreeSet<String> treeSet = new TreeSet<>(Comparator.naturalOrder());
    treeSet.addAll(testData);
    assetEqualsButNotSame(new ArrayList<>(testData));
    assetEqualsButNotSame(testData);
    assetEqualsButNotSame(new LinkedList<>(testData));
    assetEqualsButNotSame(new HashSet<>(testData));
    assetEqualsButNotSame(new LinkedHashSet<>(testData));
    assetEqualsButNotSame(treeSet);
    assetEqualsButNotSame(new CopyOnWriteArrayList<>(testData));
    assetEqualsButNotSame(new ConcurrentSkipListSet<>(testData));
    assetEqualsButNotSame(Collections.newSetFromMap(new HashMap<>()));
    assetEqualsButNotSame(ConcurrentHashMap.newKeySet(10));
    assetEqualsButNotSame(new Vector<>(testData));
    assetEqualsButNotSame(new ArrayDeque<>(testData));
    assetEqualsButNotSame(EnumSet.of(EnumSerializerTest.EnumFoo.A, EnumSerializerTest.EnumFoo.B));
    assetEqualsButNotSame(BitSet.valueOf(new byte[]{1, 2, 3}));
    assetEqualsButNotSame(new PriorityQueue<>(testData));
    assetEqualsButNotSame(Collections.singleton(1));
    assetEqualsButNotSame(Collections.singletonList(1));
  }

  private void primitiveCopyTest() {
    assertSame(Boolean.TRUE);
    assertSame(Byte.MAX_VALUE);
    assertSame(Short.MAX_VALUE);
    assertSame(Character.MAX_VALUE);
    assertSame(Integer.MAX_VALUE);
    assertSame(Long.MAX_VALUE);
    assertSame(Float.MAX_VALUE);
    assertSame(Double.MAX_VALUE);
  }

  private void timeCopyTest() {
    assertSame(new java.util.Date());
    assertSame(new java.sql.Date(System.currentTimeMillis()));
    assertSame(new Time(System.currentTimeMillis()));
    assertSame(new Timestamp(System.currentTimeMillis()));
    assertSame(LocalDate.now());
    assertSame(LocalTime.now());
    assertSame(LocalDateTime.now());
    assertSame(DateTimeUtils.truncateInstantToMicros(Instant.now()));
    assertSame(Duration.ofDays(10));
    assertSame(ZoneOffset.MAX);
    assertSame(ZonedDateTime.now());
    assertSame(Year.now());
    assertSame(YearMonth.now());
    assertSame(MonthDay.now());
    assertSame(Period.ofDays(10));
    assertSame(OffsetTime.now());
    assertSame(OffsetDateTime.now());
  }

  private void assertSame(Object obj) {
    Assert.assertSame(obj, fury.copy(obj));
  }

  private void assetEqualsButNotSame(Object obj) {
    Object newObj = fury.copy(obj);
    Assert.assertNotSame(obj, newObj);
    Assert.assertNotSame(obj, newObj);
  }


  public static class A {
    private String name;
    private B b;
    private List<Integer> list = new ArrayList<>();

    public String getName() {
      return name;
    }

    public void setName(String name) {
      this.name = name;
    }

    public B getB() {
      return b;
    }

    public void setB(B b) {
      this.b = b;
    }

    public List<Integer> getList() {
      return list;
    }

    public void setList(List<Integer> list) {
      this.list = list;
    }

    @Override
    public String toString() {
      return "A{" +
          "name='" + name + '\'' +
          ", list=" + list +
          '}';
    }
  }

  public static class B {
    private String name;
    private A a;
    private List<Integer> list = new ArrayList<>();

    public String getName() {
      return name;
    }

    public void setName(String name) {
      this.name = name;
    }

    public A getA() {
      return a;
    }

    public void setA(A a) {
      this.a = a;
    }

    public List<Integer> getList() {
      return list;
    }

    public void setList(List<Integer> list) {
      this.list = list;
    }

    @Override
    public String toString() {
      return "B{" +
          "name='" + name + '\'' +
          ", list=" + list +
          '}';
    }
  }
}
