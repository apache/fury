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

package io.fury.resolver;

import io.fury.Fury;
import io.fury.FuryTestBase;
import io.fury.Language;
import io.fury.exception.InsecureException;
import io.fury.util.Platform;
import java.rmi.server.UnicastRemoteObject;
import org.testng.Assert;
import org.testng.annotations.Test;

public class BlackListTest extends FuryTestBase {

  @Test
  public void testGetDefaultBlackList() {
    Assert.assertTrue(
        BlackList.getDefaultBlackList().contains("java.rmi.server.UnicastRemoteObject"));
    Assert.assertTrue(
        BlackList.getDefaultBlackList().contains("com.sun.jndi.rmi.registry.BindingEnumeration"));
    Assert.assertFalse(BlackList.getDefaultBlackList().contains("java.util.HashMap"));
    Assert.assertTrue(
        BlackList.getDefaultBlackList().contains(java.beans.Expression.class.getName()));
    Assert.assertTrue(
        BlackList.getDefaultBlackList().contains(UnicastRemoteObject.class.getName()));
  }

  @Test
  public void testSerializeBlackListClass() {
    for (Fury fury :
        new Fury[] {
          Fury.builder().withLanguage(Language.JAVA).requireClassRegistration(false).build(),
          Fury.builder().withLanguage(Language.JAVA).requireClassRegistration(true).build(),
          Fury.builder().withLanguage(Language.JAVA).requireClassRegistration(false).build()
        }) {
      Assert.assertThrows(
          InsecureException.class,
          () -> fury.serialize(Platform.newInstance(UnicastRemoteObject.class)));
      serDe(fury, new String[] {"a", "b"});
    }
  }
}
