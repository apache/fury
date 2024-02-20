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

package org.apache.fury.graalvm;

import java.io.Serializable;
import java.lang.reflect.InvocationHandler;
import java.lang.reflect.Method;
import java.lang.reflect.Proxy;
import java.util.function.Function;
import org.apache.fury.Fury;
import org.apache.fury.util.Preconditions;

@SuppressWarnings({"rawtypes", "unchecked"})
public class ProxyExample {
  private static class TestInvocationHandler implements InvocationHandler, Serializable {
    @Override
    public Object invoke(Object proxy, Method method, Object[] args) throws Throwable {
      return 1;
    }
  }

  static Fury fury;

  static {
    fury = Fury.builder().requireClassRegistration(true).build();
    // register and generate serializer code.
    fury.register(TestInvocationHandler.class, true);
  }

  public static void main(String[] args) {
    Function function =
        (Function)
            Proxy.newProxyInstance(
                fury.getClassLoader(), new Class[] {Function.class}, new TestInvocationHandler());
    Function deserializedFunction = (Function) fury.deserialize(fury.serialize(function));
    Preconditions.checkArgument(deserializedFunction.apply(null).equals(1));
    System.out.println("Proxy tests pass");
  }
}
