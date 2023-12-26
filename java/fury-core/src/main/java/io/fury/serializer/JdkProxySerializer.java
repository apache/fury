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

package io.fury.serializer;

import io.fury.Fury;
import io.fury.memory.MemoryBuffer;
import io.fury.util.Preconditions;
import io.fury.util.ReflectionUtils;
import java.lang.reflect.InvocationHandler;
import java.lang.reflect.Proxy;

/**
 * Serializer for jdk {@link Proxy}.
 *
 * @author chaokunyang
 */
@SuppressWarnings({"rawtypes", "unchecked"})
public class JdkProxySerializer extends Serializer {

  public JdkProxySerializer(Fury fury, Class cls) {
    super(fury, cls);
    if (cls != ReplaceStub.class) {
      Preconditions.checkArgument(ReflectionUtils.isJdkProxy(cls), "Require a jdk proxy class");
    }
  }

  @Override
  public void write(MemoryBuffer buffer, Object value) {
    fury.writeRef(buffer, Proxy.getInvocationHandler(value));
    fury.writeRef(buffer, value.getClass().getInterfaces());
  }

  @Override
  public Object read(MemoryBuffer buffer) {
    InvocationHandler invocationHandler = (InvocationHandler) fury.readRef(buffer);
    Preconditions.checkNotNull(invocationHandler);
    final Class<?>[] interfaces = (Class<?>[]) fury.readRef(buffer);
    Preconditions.checkNotNull(interfaces);
    return Proxy.newProxyInstance(fury.getClassLoader(), interfaces, invocationHandler);
  }

  public static class ReplaceStub {}
}
