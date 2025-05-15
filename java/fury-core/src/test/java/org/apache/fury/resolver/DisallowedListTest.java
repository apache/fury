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

import java.io.BufferedReader;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.nio.charset.StandardCharsets;
import java.rmi.server.UnicastRemoteObject;
import java.security.MessageDigest;
import java.util.Set;
import java.util.TreeSet;
import java.util.stream.Collectors;
import org.apache.fury.Fury;
import org.apache.fury.FuryTestBase;
import org.apache.fury.config.Language;
import org.apache.fury.exception.InsecureException;
import org.apache.fury.memory.Platform;
import org.apache.fury.reflect.ReflectionUtils;
import org.testng.Assert;
import org.testng.annotations.Test;

public class DisallowedListTest extends FuryTestBase {

  @Test
  public void testCalculateSHA256() throws Exception {
    final String disallowedListTxtPath =
        (String)
            ReflectionUtils.getDeclaredStaticFieldValue(
                DisallowedList.class, "DISALLOWED_LIST_TXT_PATH");
    try (InputStream is =
        DisallowedList.class.getClassLoader().getResourceAsStream(disallowedListTxtPath)) {
      assert is != null;
      Set<String> set =
          new BufferedReader(new InputStreamReader(is, StandardCharsets.UTF_8))
              .lines()
              .filter(line -> !line.isEmpty() && !line.startsWith("#"))
              .collect(Collectors.toSet());
      MessageDigest digest = MessageDigest.getInstance("SHA-256");
      byte[] hashBytes =
          digest.digest(String.join(",", new TreeSet<>(set)).getBytes(StandardCharsets.UTF_8));
      StringBuilder hexString = new StringBuilder();
      for (byte b : hashBytes) {
        String hex = Integer.toHexString(0xff & b);
        if (hex.length() == 1) {
          hexString.append('0');
        }
        hexString.append(hex);
      }
      System.out.println("SHA256 HASH for disallowed.txt is " + hexString);

      Assert.assertEquals(
          hexString.toString(),
          ReflectionUtils.getDeclaredStaticFieldValue(DisallowedList.class, "SHA256_HASH"),
          "Please update `DisallowedList#SHA256_HASH` with the above output hash value.");
    }
  }

  @Test
  public void testCheckHitDisallowedList() {
    // Hit the disallowed list.
    Assert.assertThrows(
        InsecureException.class,
        () -> DisallowedList.checkNotInDisallowedList("java.rmi.server.UnicastRemoteObject"));
    Assert.assertThrows(
        InsecureException.class,
        () ->
            DisallowedList.checkNotInDisallowedList(
                "com.sun.jndi.rmi.registry.BindingEnumeration"));
    Assert.assertThrows(
        InsecureException.class,
        () -> DisallowedList.checkNotInDisallowedList(java.beans.Expression.class.getName()));
    Assert.assertThrows(
        InsecureException.class,
        () -> DisallowedList.checkNotInDisallowedList(UnicastRemoteObject.class.getName()));

    // Not in the disallowed list.
    DisallowedList.checkNotInDisallowedList("java.util.HashMap");
  }

  @Test
  public void testSerializeDisallowedClass() {
    Fury[] allFury = new Fury[3];
    for (int i = 0; i < 3; i++) {
      boolean requireClassRegistration = i % 2 == 0;
      Fury fury =
          Fury.builder()
              .withLanguage(Language.JAVA)
              .requireClassRegistration(requireClassRegistration)
              .build();
      if (requireClassRegistration) {
        // Registered or unregistered Classes should be subject to disallowed list restrictions.
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
