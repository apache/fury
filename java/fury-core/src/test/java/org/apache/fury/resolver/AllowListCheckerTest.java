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

import static org.testng.Assert.*;

import org.apache.fury.Fury;
import org.apache.fury.ThreadLocalFury;
import org.apache.fury.ThreadSafeFury;
import org.apache.fury.exception.InsecureException;
import org.testng.annotations.Test;

public class AllowListCheckerTest {

  @Test
  public void testCheckClass() {
    {
      Fury fury = Fury.builder().requireClassRegistration(false).build();
      AllowListChecker checker = new AllowListChecker(AllowListChecker.CheckLevel.STRICT);
      fury.getClassResolver().setClassChecker(checker);
      assertThrows(InsecureException.class, () -> fury.serialize(new AllowListCheckerTest()));
      checker.allowClass(AllowListCheckerTest.class.getName());
      byte[] bytes = fury.serialize(new AllowListCheckerTest());
      checker.addListener(fury.getClassResolver());
      checker.disallowClass(AllowListCheckerTest.class.getName());
      assertThrows(InsecureException.class, () -> fury.serialize(new AllowListCheckerTest()));
      assertThrows(InsecureException.class, () -> fury.deserialize(bytes));
    }
    {
      Fury fury = Fury.builder().requireClassRegistration(false).build();
      AllowListChecker checker = new AllowListChecker(AllowListChecker.CheckLevel.WARN);
      fury.getClassResolver().setClassChecker(checker);
      checker.addListener(fury.getClassResolver());
      byte[] bytes = fury.serialize(new AllowListCheckerTest());
      checker.disallowClass(AllowListCheckerTest.class.getName());
      assertThrows(InsecureException.class, () -> fury.serialize(new AllowListCheckerTest()));
      assertThrows(InsecureException.class, () -> fury.deserialize(bytes));
    }
  }

  @Test
  public void testCheckClassWildcard() {
    {
      Fury fury = Fury.builder().requireClassRegistration(false).build();
      AllowListChecker checker = new AllowListChecker(AllowListChecker.CheckLevel.STRICT);
      fury.getClassResolver().setClassChecker(checker);
      checker.addListener(fury.getClassResolver());
      assertThrows(InsecureException.class, () -> fury.serialize(new AllowListCheckerTest()));
      checker.allowClass("org.apache.fury.*");
      byte[] bytes = fury.serialize(new AllowListCheckerTest());
      checker.disallowClass("org.apache.fury.*");
      assertThrows(InsecureException.class, () -> fury.serialize(new AllowListCheckerTest()));
      assertThrows(InsecureException.class, () -> fury.deserialize(bytes));
    }
    {
      Fury fury = Fury.builder().requireClassRegistration(false).build();
      AllowListChecker checker = new AllowListChecker(AllowListChecker.CheckLevel.WARN);
      fury.getClassResolver().setClassChecker(checker);
      checker.addListener(fury.getClassResolver());
      byte[] bytes = fury.serialize(new AllowListCheckerTest());
      checker.disallowClass("org.apache.fury.*");
      assertThrows(InsecureException.class, () -> fury.serialize(new AllowListCheckerTest()));
      assertThrows(InsecureException.class, () -> fury.deserialize(bytes));
    }
  }

  @Test
  public void testThreadSafeFury() {
    AllowListChecker checker = new AllowListChecker(AllowListChecker.CheckLevel.STRICT);
    ThreadSafeFury fury =
        new ThreadLocalFury(
            classLoader -> {
              Fury f =
                  Fury.builder()
                      .requireClassRegistration(false)
                      .withClassLoader(classLoader)
                      .build();
              f.getClassResolver().setClassChecker(checker);
              checker.addListener(f.getClassResolver());
              return f;
            });
    checker.allowClass("org.apache.fury.*");
    byte[] bytes = fury.serialize(new AllowListCheckerTest());
    checker.disallowClass("org.apache.fury.*");
    assertThrows(InsecureException.class, () -> fury.serialize(new AllowListCheckerTest()));
    assertThrows(InsecureException.class, () -> fury.deserialize(bytes));
  }
}
