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

import static org.testng.Assert.assertEquals;

import java.io.Serializable;
import java.lang.reflect.InvocationHandler;
import java.lang.reflect.Method;
import java.lang.reflect.Proxy;
import java.util.function.Function;
import org.apache.fury.Fury;
import org.apache.fury.FuryTestBase;
import org.apache.fury.config.Language;
import org.testng.annotations.Test;

@SuppressWarnings({"unchecked", "rawtypes"})
public class JdkProxySerializerTest extends FuryTestBase {

  private static class TestInvocationHandler implements InvocationHandler, Serializable {

    @Override
    public Object invoke(Object proxy, Method method, Object[] args) throws Throwable {
      return 1;
    }
  }

  @Test(dataProvider = "referenceTrackingConfig")
  public void testJdkProxy(boolean referenceTracking) {
    Fury fury =
        Fury.builder()
            .withLanguage(Language.JAVA)
            .withRefTracking(referenceTracking)
            .requireClassRegistration(false)
            .build();
    Function function =
        (Function)
            Proxy.newProxyInstance(
                fury.getClassLoader(), new Class[] {Function.class}, new TestInvocationHandler());
    Function deserializedFunction = (Function) fury.deserialize(fury.serialize(function));
    assertEquals(deserializedFunction.apply(null), 1);
  }

  private static class RefTestInvocationHandler implements InvocationHandler, Serializable {

    private Function proxy;

    @Override
    public Object invoke(Object proxy, Method method, Object[] args) throws Throwable {
      if (method.getName().equals("equals")) {
        return args[0] == this.proxy;
      }
      return "Hello world from "
          + (proxy == null
              ? "null"
              : proxy.getClass().getName() + "@" + System.identityHashCode(proxy));
    }

    private void setProxy(Function myProxy) {
      this.proxy = myProxy;
    }

    private Function getProxy() {
      return proxy;
    }
  }

  @Test
  public void testJdkProxyRef() {
    Fury fury =
        Fury.builder()
            .withLanguage(Language.JAVA)
            .withRefTracking(true)
            .requireClassRegistration(false)
            .build();
    RefTestInvocationHandler hdlr = new RefTestInvocationHandler();
    Function function =
        (Function)
            Proxy.newProxyInstance(fury.getClassLoader(), new Class[] {Function.class}, hdlr);
    hdlr.setProxy(function);
    assertEquals(hdlr.getProxy(), function);

    Function deserializedFunction = (Function) fury.deserialize(fury.serialize(function));
    RefTestInvocationHandler deserializedHandler =
        (RefTestInvocationHandler) Proxy.getInvocationHandler(deserializedFunction);
    assertEquals(deserializedHandler.getProxy(), deserializedFunction);
  }
}
