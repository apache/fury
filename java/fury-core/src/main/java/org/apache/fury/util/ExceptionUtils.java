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

package org.apache.fury.util;

import org.apache.fury.Fury;
import org.apache.fury.collection.ObjectArray;
import org.apache.fury.exception.DeserializationException;
import org.apache.fury.memory.Platform;
import org.apache.fury.reflect.ReflectionUtils;
import org.apache.fury.resolver.MapRefResolver;

import java.lang.reflect.Field;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

/** Util for java exceptions. */
public class ExceptionUtils {
  private static final Field detailMessageField;

  static {
    try {
      detailMessageField = Throwable.class.getDeclaredField("detailMessage");
    } catch (NoSuchFieldException e) {
      throw new RuntimeException(e);
    }
  }

  /**
   * Try to set `StackOverflowError` exception message. Returns passed exception if set succeed, or
   * null if failed.
   */
  public static StackOverflowError trySetStackOverflowErrorMessage(
      StackOverflowError e, String message) {
    if (detailMessageField != null) {
      ReflectionUtils.setObjectFieldValue(e, detailMessageField, message);
      return e;
    } else {
      return null;
    }
  }

  public static RuntimeException handleReadFailed(Fury fury, Throwable t) {
    if (fury.getRefResolver() instanceof MapRefResolver) {
        List<Object> exceptionObjects = getExceptionObjects(fury);
        throw new DeserializationException(exceptionObjects, t);
    } else {
      Platform.throwException(t);
      throw new IllegalStateException("unreachable");
    }
  }

  public static List<Object> getExceptionObjects(Fury fury) {
      ObjectArray readObjects = ((MapRefResolver) fury.getRefResolver()).getReadObjects();
      // carry with read objects for better trouble shooting.
      List<Object> objects = Arrays.asList(readObjects.objects).subList(0, readObjects.size);
      switch (fury.getExceptionLogMode()) {
        case NONE_PRINT:
          return new ArrayList<>();
        case SAMPLE_PRINT:
            return systematicSample(objects, fury.getLogSampleStep());
//            return objects.subList(0, 10);
        default:
          return objects;
      }
  }

    public static <T> List<T> systematicSample(List<T> list, int step) {
        List<T> result = new ArrayList<>();
        for (int i = 0; i < list.size(); i += step) {
            result.add(list.get(i));
        }
        return result;
    }

  public static void ignore(Object... args) {}
}
