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

import java.lang.reflect.Field;
import java.lang.reflect.InvocationHandler;
import java.lang.reflect.Proxy;
import org.apache.fury.Fury;
import org.apache.fury.memory.MemoryBuffer;
import org.apache.fury.memory.Platform;
import org.apache.fury.reflect.ReflectionUtils;
import org.apache.fury.resolver.RefResolver;
import org.apache.fury.util.Preconditions;

/** Serializer for jdk {@link Proxy}. */
@SuppressWarnings({"rawtypes", "unchecked"})
public class JdkProxySerializer extends Serializer {

  // Make offset compatible with graalvm native image.
  private static final Field FIELD;
  private static final long PROXY_HANDLER_FIELD_OFFSET;

  static {
    FIELD = ReflectionUtils.getField(Proxy.class, InvocationHandler.class);
    PROXY_HANDLER_FIELD_OFFSET = Platform.objectFieldOffset(FIELD);
  }

  private static final InvocationHandler STUB_HANDLER =
      (proxy, method, args) -> {
        throw new IllegalStateException("Deserialization stub handler still active");
      };

  public JdkProxySerializer(Fury fury, Class cls) {
    super(fury, cls);
    if (cls != ReplaceStub.class) {
      Preconditions.checkArgument(ReflectionUtils.isJdkProxy(cls), "Require a jdk proxy class");
    }
  }

  @Override
  public void write(MemoryBuffer buffer, Object value) {
    fury.writeRef(buffer, value.getClass().getInterfaces());
    fury.writeRef(buffer, Proxy.getInvocationHandler(value));
  }

  @Override
  public Object read(MemoryBuffer buffer) {
    final RefResolver resolver = fury.getRefResolver();
    final int refId = resolver.lastPreservedRefId();
    final Class<?>[] interfaces = (Class<?>[]) fury.readRef(buffer);
    Preconditions.checkNotNull(interfaces);
    Object proxy = Proxy.newProxyInstance(fury.getClassLoader(), interfaces, STUB_HANDLER);
    resolver.setReadObject(refId, proxy);
    InvocationHandler invocationHandler = (InvocationHandler) fury.readRef(buffer);
    Preconditions.checkNotNull(invocationHandler);
    Platform.putObject(proxy, PROXY_HANDLER_FIELD_OFFSET, invocationHandler);
    return proxy;
  }

  public static class ReplaceStub {}
}
