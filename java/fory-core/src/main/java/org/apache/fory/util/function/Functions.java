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

package org.apache.fory.util.function;

import java.io.Serializable;
import java.lang.invoke.MethodHandle;
import java.lang.invoke.MethodHandles;
import java.lang.invoke.SerializedLambda;
import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.WeakHashMap;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Predicate;
import org.apache.fory.collection.Tuple2;
import org.apache.fory.reflect.ReflectionUtils;
import org.apache.fory.util.GraalvmSupport;
import org.apache.fory.util.Preconditions;
import org.apache.fory.util.unsafe._JDKAccess;

/** Utility for lambda functions. */
public class Functions {
  /** Returns true if the specified class is a lambda. */
  public static boolean isLambda(Class<?> clz) {
    Preconditions.checkNotNull(clz);
    return clz.getName().indexOf('/') >= 0;
  }

  public static List<Object> extractCapturedVariables(Serializable closure) {
    return extractCapturedVariables(closure, o -> true);
  }

  public static List<Object> extractCapturedVariables(
      Serializable closure, Predicate<Object> predicate) {
    Preconditions.checkArgument(Functions.isLambda(closure.getClass()));
    Method writeReplace = ReflectionUtils.findMethods(closure.getClass(), "writeReplace").get(0);
    writeReplace.setAccessible(true);
    SerializedLambda serializedLambda;
    try {
      serializedLambda = (SerializedLambda) writeReplace.invoke(closure);
    } catch (Exception e) {
      throw new IllegalStateException();
    }
    List<Object> variables = new ArrayList<>();
    for (int i = 0; i < serializedLambda.getCapturedArgCount(); i++) {
      Object capturedArg = serializedLambda.getCapturedArg(i);
      if (predicate.test(capturedArg)) {
        variables.add(capturedArg);
      }
    }
    return variables;
  }

  public static Object makeGetterFunction(Class<?> cls, String methodName) {
    try {
      return makeGetterFunction(cls.getDeclaredMethod(methodName));
    } catch (NoSuchMethodException e) {
      throw new RuntimeException(e);
    }
  }

  private static final Map<Tuple2<Method, Class<?>>, Object> map =
      GraalvmSupport.isGraalBuildtime() ? new ConcurrentHashMap<>() : new WeakHashMap<>();

  public static Object makeGetterFunction(Method method) {
    return map.computeIfAbsent(
        Tuple2.of(method, Object.class),
        k -> {
          MethodHandles.Lookup lookup = _JDKAccess._trustedLookup(method.getDeclaringClass());
          try {
            // Why `lookup.findGetter` doesn't work?
            // MethodHandle handle = lookup.findGetter(field.getDeclaringClass(), field.getName(),
            // field.getType());
            MethodHandle handle = lookup.unreflect(method);
            return _JDKAccess.makeGetterFunction(lookup, handle, method.getReturnType());
          } catch (IllegalAccessException ex) {
            throw new RuntimeException(ex);
          }
        });
  }

  public static Object makeGetterFunction(Method method, Class<?> returnType) {
    return map.computeIfAbsent(
        Tuple2.of(method, returnType),
        k -> {
          MethodHandles.Lookup lookup = _JDKAccess._trustedLookup(method.getDeclaringClass());
          try {
            MethodHandle handle = lookup.unreflect(method);
            return _JDKAccess.makeGetterFunction(lookup, handle, returnType);
          } catch (IllegalAccessException ex) {
            throw new RuntimeException(ex);
          }
        });
  }

  public static Tuple2<Class<?>, String> getterMethodInfo(Class<?> type) {
    return _JDKAccess.getterMethodInfo(type);
  }
}
