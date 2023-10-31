/*
 * Copyright 2023 The Fury Authors
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
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

import com.google.common.collect.ImmutableBiMap;
import com.google.common.collect.ImmutableMap;
import io.fury.util.FieldAccessor;
import io.fury.util.ReflectionUtils;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.ObjectOutputStream;
import java.util.ArrayList;
import java.util.function.Supplier;

/**
 * Test utils.
 *
 * @author chaokunyang
 */
public class TestUtils {
  @SuppressWarnings("unchecked")
  public static <T> T getFieldValue(Object obj, String fieldName) {
    return (T)
        FieldAccessor.createAccessor(ReflectionUtils.getField(obj.getClass(), fieldName)).get(obj);
  }

  public static <K, V> ImmutableMap<K, V> mapOf(K k1, V v1) {
    return ImmutableBiMap.of(k1, v1);
  }

  /**
   * Trigger OOM for SoftGC by allocate more memory.
   *
   * @param predicate whether stop Trigger OOM.
   */
  public static void triggerOOMForSoftGC(Supplier<Boolean> predicate) {
    System.gc();
    while (predicate.get()) {
      triggerOOM();
      System.gc();
      System.out.printf("Wait gc.");
      try {
        Thread.sleep(50);
      } catch (InterruptedException e1) {
        throw new RuntimeException(e1);
      }
    }
  }

  private static void triggerOOM() {
    while (true) {
      // Force an OOM
      try {
        final ArrayList<Object[]> allocations = new ArrayList<>();
        int size;
        while ((size =
                Math.min(Math.abs((int) Runtime.getRuntime().freeMemory()), Integer.MAX_VALUE))
            > 0) allocations.add(new Object[size]);
      } catch (OutOfMemoryError e) {
        System.out.println("Met OOM.");
        break;
      }
    }
  }

  public static byte[] jdkSerialize(Object data) {
    ByteArrayOutputStream bas = new ByteArrayOutputStream();
    jdkSerialize(bas, data);
    return bas.toByteArray();
  }

  public static void jdkSerialize(ByteArrayOutputStream bas, Object data) {
    bas.reset();
    try (ObjectOutputStream objectOutputStream = new ObjectOutputStream(bas)) {
      objectOutputStream.writeObject(data);
      objectOutputStream.flush();
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }
}
