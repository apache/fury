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

package io.fury.util.unsafe;

import com.google.common.base.Preconditions;
import com.google.common.primitives.Primitives;
import io.fury.util.Utils;
import java.lang.invoke.CallSite;
import java.lang.invoke.LambdaMetafactory;
import java.lang.invoke.MethodHandle;
import java.lang.invoke.MethodHandles.Lookup;
import java.lang.invoke.MethodType;
import java.lang.reflect.Field;
import java.lang.reflect.Method;
import sun.misc.Unsafe;

// CHECKSTYLE.OFF:TypeName
public class _JDKAccess {
  // CHECKSTYLE.OFF:TypeName
  public static final int JAVA_VERSION;
  public static final boolean OPEN_J9;
  public static final Unsafe UNSAFE;
  public static final Class<?> _INNER_UNSAFE_CLASS;
  public static final Object _INNER_UNSAFE;

  static {
    String property = System.getProperty("java.specification.version");
    if (property.startsWith("1.")) {
      property = property.substring(2);
    }
    String jmvName = System.getProperty("java.vm.name", "");
    OPEN_J9 = jmvName.contains("OpenJ9");
    JAVA_VERSION = Integer.parseInt(property);

    Unsafe unsafe;
    try {
      Field unsafeField = Unsafe.class.getDeclaredField("theUnsafe");
      unsafeField.setAccessible(true);
      unsafe = (Unsafe) unsafeField.get(null);
    } catch (Throwable cause) {
      throw new UnsupportedOperationException("Unsafe is not supported in this platform.");
    }
    UNSAFE = unsafe;
    if (JAVA_VERSION >= 11) {
      try {
        Field theInternalUnsafeField = Unsafe.class.getDeclaredField("theInternalUnsafe");
        theInternalUnsafeField.setAccessible(true);
        _INNER_UNSAFE = theInternalUnsafeField.get(null);
        _INNER_UNSAFE_CLASS = _INNER_UNSAFE.getClass();
      } catch (Exception e) {
        throw new RuntimeException(e);
      }
    } else {
      _INNER_UNSAFE_CLASS = null;
      _INNER_UNSAFE = null;
    }
  }

  // CHECKSTYLE.OFF:MethodName

  public static Lookup _trustedLookup(Class<?> objectClass) {
    // CHECKSTYLE.ON:MethodName
    return _Lookup._trustedLookup(objectClass);
  }

  public static <T> T tryMakeFunction(
      Lookup lookup, MethodHandle handle, Class<T> functionInterface) {
    try {
      return makeFunction(lookup, handle, functionInterface);
    } catch (Throwable e) {
      Utils.ignore(e);
      throw new IllegalStateException();
    }
  }

  @SuppressWarnings("unchecked")
  public static <T> T makeFunction(Lookup lookup, MethodHandle handle, Method methodToImpl) {
    MethodType methodType = handle.type();
    Class<?>[] paramTypes = new Class[methodType.parameterCount()];
    for (int i = 0; i < paramTypes.length; i++) {
      Class<?> t = methodType.parameterType(i);
      if (t.isPrimitive()) {
        t = Primitives.wrap(t);
      }
      paramTypes[i] = t;
    }
    MethodType instantiatedMethodType = MethodType.methodType(methodType.returnType(), paramTypes);
    MethodType methodToImplType =
        MethodType.methodType(methodToImpl.getReturnType(), methodToImpl.getParameterTypes());
    try {
      // Faster than handle.invokeExact.
      CallSite callSite =
          LambdaMetafactory.metafactory(
              lookup,
              methodToImpl.getName(),
              MethodType.methodType(methodToImpl.getDeclaringClass()),
              methodToImplType,
              handle,
              instantiatedMethodType);
      return (T) callSite.getTarget().invokeExact();
    } catch (Throwable e) {
      UNSAFE.throwException(e);
      throw new IllegalStateException(e);
    }
  }

  public static <T> T makeFunction(Lookup lookup, MethodHandle handle, Class<T> functionInterface) {
    String invokedName = "apply";
    try {
      Method method = null;
      Method[] methods = functionInterface.getMethods();
      for (Method interfaceMethod : methods) {
        if (interfaceMethod.getName().equals(invokedName)) {
          method = interfaceMethod;
          break;
        }
      }
      if (method == null) {
        Preconditions.checkArgument(methods.length == 1);
        method = methods[0];
        invokedName = method.getName();
      }
      MethodType interfaceType =
          MethodType.methodType(method.getReturnType(), method.getParameterTypes());
      // Faster than handle.invokeExact.
      CallSite callSite =
          LambdaMetafactory.metafactory(
              lookup,
              invokedName,
              MethodType.methodType(functionInterface),
              interfaceType,
              handle,
              interfaceType);
      // FIXME(chaokunyang) why use invokeExact will fail.
      return (T) callSite.getTarget().invoke();
    } catch (Throwable e) {
      UNSAFE.throwException(e);
      throw new IllegalStateException(e);
    }
  }
}
