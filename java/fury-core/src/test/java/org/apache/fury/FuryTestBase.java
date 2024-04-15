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

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Function;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;
import org.apache.fury.config.CompatibleMode;
import org.apache.fury.config.FuryBuilder;
import org.apache.fury.config.Language;
import org.apache.fury.io.ClassLoaderObjectInputStream;
import org.apache.fury.memory.MemoryBuffer;
import org.apache.fury.resolver.MetaContext;
import org.apache.fury.serializer.BufferObject;
import org.apache.fury.util.Platform;
import org.apache.fury.util.ReflectionUtils;
import org.testng.Assert;
import org.testng.annotations.DataProvider;

/** Fury unit test base class. */
@SuppressWarnings("unchecked")
public abstract class FuryTestBase {
  private static final ThreadLocal<Fury> javaFuryLocal =
      ThreadLocal.withInitial(
          () -> Fury.builder().withLanguage(Language.JAVA).requireClassRegistration(false).build());

  public static Fury getJavaFury() {
    return javaFuryLocal.get();
  }

  public static FuryBuilder builder() {
    return Fury.builder().withLanguage(Language.JAVA).requireClassRegistration(false);
  }

  @DataProvider
  public static Object[][] referenceTrackingConfig() {
    return new Object[][] {{false}, {true}};
  }

  @DataProvider
  public static Object[][] trackingRefFury() {
    return new Object[][] {
      {
        Fury.builder()
            .withLanguage(Language.JAVA)
            .withRefTracking(true)
            .withCodegen(false)
            .requireClassRegistration(false)
            .build()
      },
      {
        Fury.builder()
            .withLanguage(Language.JAVA)
            .withRefTracking(false)
            .withCodegen(false)
            .requireClassRegistration(false)
            .build()
      }
    };
  }

  @DataProvider
  public static Object[][] endian() {
    return new Object[][] {{false}, {true}};
  }

  @DataProvider
  public static Object[][] enableCodegen() {
    return new Object[][] {{false}, {true}};
  }

  @DataProvider
  public static Object[][] compressNumber() {
    return new Object[][] {{false}, {true}};
  }

  @DataProvider
  public static Object[][] compressNumberAndCodeGen() {
    return new Object[][] {{false, false}, {true, false}, {false, true}, {true, true}};
  }

  @DataProvider
  public static Object[][] refTrackingAndCompressNumber() {
    return new Object[][] {{false, false}, {true, false}, {false, true}, {true, true}};
  }

  @DataProvider
  public static Object[][] crossLanguageReferenceTrackingConfig() {
    return new Object[][] {
      {false, Language.JAVA},
      {true, Language.JAVA},
      {false, Language.XLANG},
      {true, Language.XLANG}
    };
  }

  @DataProvider(name = "javaFury")
  public static Object[][] javaFuryConfig() {
    return new Object[][] {
      {
        Fury.builder()
            .withLanguage(Language.JAVA)
            .withRefTracking(true)
            .withCodegen(false)
            .requireClassRegistration(false)
            .build()
      },
      {
        Fury.builder()
            .withLanguage(Language.JAVA)
            .withRefTracking(false)
            .withCodegen(false)
            .requireClassRegistration(false)
            .build()
      },
      {
        Fury.builder()
            .withLanguage(Language.JAVA)
            .withRefTracking(true)
            .withCodegen(true)
            .requireClassRegistration(false)
            .build()
      },
      {
        Fury.builder()
            .withLanguage(Language.JAVA)
            .withRefTracking(false)
            .withCodegen(true)
            .requireClassRegistration(false)
            .build()
      },
    };
  }

  @DataProvider
  public static Object[][] javaFuryKVCompatible() {
    return new Object[][] {
      {
        Fury.builder()
            .withLanguage(Language.JAVA)
            .withRefTracking(true)
            .withCodegen(false)
            .withCompatibleMode(CompatibleMode.COMPATIBLE)
            .requireClassRegistration(false)
            .build()
      },
      {
        Fury.builder()
            .withLanguage(Language.JAVA)
            .withRefTracking(false)
            .withCodegen(false)
            .withCompatibleMode(CompatibleMode.COMPATIBLE)
            .requireClassRegistration(false)
            .build()
      },
      {
        Fury.builder()
            .withLanguage(Language.JAVA)
            .withRefTracking(true)
            .withCodegen(true)
            .withCompatibleMode(CompatibleMode.COMPATIBLE)
            .requireClassRegistration(false)
            .build()
      },
      {
        Fury.builder()
            .withLanguage(Language.JAVA)
            .withRefTracking(false)
            .withCodegen(true)
            .withCompatibleMode(CompatibleMode.COMPATIBLE)
            .requireClassRegistration(false)
            .build()
      },
    };
  }

  public static void serDeCheckSerializerAndEqual(Fury fury, Object obj, String classRegex) {
    Assert.assertEquals(serDeCheckSerializer(fury, obj, classRegex), obj);
  }

  public static <T> T serDeCheckSerializer(Fury fury, Object obj, String classRegex) {
    byte[] bytes = fury.serialize(obj);
    String serializerName = fury.getClassResolver().getSerializerClass(obj.getClass()).getName();
    Matcher matcher = Pattern.compile(classRegex).matcher(serializerName);
    Assert.assertTrue(matcher.find());
    return (T) fury.deserialize(bytes);
  }

  public static Object serDe(Fury fury1, Fury fury2, Object obj) {
    byte[] bytes = fury1.serialize(obj);
    return fury2.deserialize(bytes);
  }

  public static Object serDeCheck(Fury fury1, Fury fury2, Object obj) {
    Object o = serDe(fury1, fury2, obj);
    Assert.assertEquals(o, obj);
    return o;
  }

  public static Object serDeCheck(Fury fury, Object obj) {
    Object o = serDe(fury, obj);
    Assert.assertEquals(o, obj);
    return o;
  }

  public static <T> T serDe(Fury fury, T obj) {
    try {
      byte[] bytes = fury.serialize(obj);
      return (T) (fury.deserialize(bytes));
    } catch (Throwable t) {
      // Catch for add breakpoint for debugging.
      throw t;
    }
  }

  public static Object serDe(Fury fury1, Fury fury2, MemoryBuffer buffer, Object obj) {
    fury1.serialize(buffer, obj);
    return fury2.deserialize(buffer);
  }

  public static Object serDeCheckIndex(Fury fury1, Fury fury2, MemoryBuffer buffer, Object obj) {
    fury1.serialize(buffer, obj);
    Object newObj = fury2.deserialize(buffer);
    Assert.assertEquals(buffer.writerIndex(), buffer.readerIndex());
    return newObj;
  }

  public static void roundCheck(Fury fury1, Fury fury2, Object o) {
    roundCheck(fury1, fury2, o, Function.identity());
  }

  public static void roundCheck(
      Fury fury1, Fury fury2, Object o, Function<Object, Object> compareHook) {
    byte[] bytes1 = fury1.serialize(o);
    Object o1 = fury2.deserialize(bytes1);
    Assert.assertEquals(compareHook.apply(o1), compareHook.apply(o));
    byte[] bytes2 = fury2.serialize(o1);
    Object o2 = fury1.deserialize(bytes2);
    Assert.assertEquals(compareHook.apply(o2), compareHook.apply(o));
  }

  public static Object serDeMetaShared(Fury fury, Object obj) {
    MetaContext context = new MetaContext();
    fury.getSerializationContext().setMetaContext(context);
    byte[] bytes = fury.serialize(obj);
    fury.getSerializationContext().setMetaContext(context);
    return fury.deserialize(bytes);
  }

  public static byte[] jdkSerialize(Object o) {
    ByteArrayOutputStream bos = new ByteArrayOutputStream();
    try (ObjectOutputStream objectOutputStream = new ObjectOutputStream(bos)) {
      objectOutputStream.writeObject(o);
      objectOutputStream.flush();
      return bos.toByteArray();
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }

  public static Object jdkDeserialize(byte[] data) {
    try (ByteArrayInputStream bis = new ByteArrayInputStream(data);
        ObjectInputStream objectInputStream =
            new ClassLoaderObjectInputStream(Thread.currentThread().getContextClassLoader(), bis)) {
      return objectInputStream.readObject();
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }

  public static Object serDeOutOfBand(AtomicInteger counter, Fury fury1, Fury fury2, Object obj) {
    List<BufferObject> bufferObjects = new ArrayList<>();
    byte[] bytes =
        fury1.serialize(
            obj,
            o -> {
              if (counter.incrementAndGet() % 2 == 0) {
                bufferObjects.add(o);
                return false;
              } else {
                return true;
              }
            });
    List<MemoryBuffer> buffers =
        bufferObjects.stream().map(BufferObject::toBuffer).collect(Collectors.toList());
    return fury2.deserialize(bytes, buffers);
  }

  /** Update serialization depth by <code>diff</code>. */
  protected void increaseFuryDepth(Fury fury, int diff) {
    long offset = ReflectionUtils.getFieldOffset(Fury.class, "depth");
    int depth = Platform.getInt(fury, offset);
    Platform.putInt(fury, offset, depth + diff);
  }
}
