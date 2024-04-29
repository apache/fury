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

import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import org.apache.fury.Fury;
import org.apache.fury.FuryTestBase;
import org.apache.fury.config.Language;
import org.apache.fury.memory.MemoryBuffer;
import org.apache.fury.test.bean.Foo;
import org.testng.Assert;
import org.testng.annotations.Test;

public class UserContextTest extends FuryTestBase {

  public static class StringUserContextResolver extends UserContextResolver {

    public String data;

    public StringUserContextResolver(Fury fury) {
      super(fury);
    }

    @Override
    void write(MemoryBuffer buffer) {
      fury.writeJavaStringRef(buffer, data);
    }

    @Override
    void read(MemoryBuffer buffer) {
      data = fury.readJavaStringRef(buffer);
    }
  }

  private Fury buildFury() {
    return Fury.builder()
        .withLanguage(Language.JAVA)
        .withUserContextShare(true)
        .requireClassRegistration(false)
        .build();
  }

  @Test
  public void checkShareUserContext() {
    Fury writrfury = buildFury();
    final List<StringUserContextResolver> resolvers = getResolver(writrfury, 4, false);
    final int size = resolvers.size();
    for (int i = 0; i < size; i++) {
      writrfury
          .getSerializationContext()
          .registerUserContextResolver(String.valueOf(i), resolvers.get(i));
    }
    final Foo o = Foo.create();
    final byte[] bytes = writrfury.serialize(o);
    Fury readFury = buildFury();
    final List<StringUserContextResolver> readResolvers = getResolver(readFury, 4, true);
    for (int i = 0; i < size; i++) {
      readFury
          .getSerializationContext()
          .registerUserContextResolver(String.valueOf(i), readResolvers.get(i));
    }
    Assert.assertEquals(readFury.deserialize(bytes), o);
    for (int i = 0; i < size; i++) {
      Assert.assertEquals(resolvers.get(i).data, readResolvers.get(i).data);
    }
  }

  private List<StringUserContextResolver> getResolver(Fury fury, int num, boolean empty) {
    return IntStream.range(0, num)
        .mapToObj(
            idx -> {
              final StringUserContextResolver userContextResolver =
                  new StringUserContextResolver(fury);
              if (!empty) {
                userContextResolver.data = "data" + idx;
              }
              return userContextResolver;
            })
        .collect(Collectors.toList());
  }
}
