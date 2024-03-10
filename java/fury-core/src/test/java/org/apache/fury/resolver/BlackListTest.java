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

package org.apache.fury.resolver;

import java.rmi.server.UnicastRemoteObject;
import org.apache.fury.Fury;
import org.apache.fury.FuryTestBase;
import org.apache.fury.config.Language;
import org.apache.fury.exception.InsecureException;
import org.apache.fury.util.Platform;
import org.testng.Assert;
import org.testng.annotations.Test;

public class BlackListTest extends FuryTestBase {

  @Test
  public void testCheckHitBlackList() {
    // Hit the blacklist.
    Assert.assertThrows(
        InsecureException.class,
        () -> BlackList.checkNotInBlackList("java.rmi.server.UnicastRemoteObject"));
    Assert.assertThrows(
        InsecureException.class,
        () -> BlackList.checkNotInBlackList("com.sun.jndi.rmi.registry.BindingEnumeration"));
    Assert.assertThrows(
        InsecureException.class,
        () -> BlackList.checkNotInBlackList(java.beans.Expression.class.getName()));
    Assert.assertThrows(
        InsecureException.class,
        () -> BlackList.checkNotInBlackList(UnicastRemoteObject.class.getName()));

    // Not in the blacklist.
    BlackList.checkNotInBlackList("java.util.HashMap");
  }

  @Test
  public void testSerializeBlackListClass() {
    Fury[] allFury = new Fury[3];
    for (int i = 0; i < 3; i++) {
      boolean requireClassRegistration = i % 2 == 0;
      Fury fury =
          Fury.builder()
              .withLanguage(Language.JAVA)
              .requireClassRegistration(requireClassRegistration)
              .build();
      if (requireClassRegistration) {
        // Registered or unregistered Classes should be subject to blacklist restrictions.
        fury.register(UnicastRemoteObject.class);
      }
      allFury[i] = fury;
    }

    for (Fury fury : allFury) {
      Assert.assertThrows(
          InsecureException.class,
          () -> fury.serialize(Platform.newInstance(UnicastRemoteObject.class)));
      serDe(fury, new String[] {"a", "b"});
    }
  }
}
