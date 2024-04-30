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

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import org.apache.fury.Fury;
import org.apache.fury.FuryTestBase;
import org.apache.fury.config.Language;
import org.apache.fury.memory.MemoryBuffer;
import org.apache.fury.serializer.ObjectSerializer;
import org.apache.fury.serializer.Serializer;
import org.apache.fury.test.bean.Foo;
import org.testng.Assert;
import org.testng.annotations.Test;

public class UserContextTest extends FuryTestBase {

  private static final int NUM = 10;

  public static class StringUserContextResolver extends UserContextResolver {

    public String data;

    public StringUserContextResolver(Fury fury) {
      super(fury);
    }

    @Override
    public void write(MemoryBuffer buffer) {
      fury.writeJavaStringRef(buffer, data);
    }

    @Override
    public void read(MemoryBuffer buffer) {
      data = fury.readJavaStringRef(buffer);
    }

    @Override
    public void reset() {
      data = null;
    }
  }

  public static class FooSerializer extends Serializer<Foo> {

    private final ObjectSerializer<Foo> serializer;

    public FooSerializer(Fury fury, Class<Foo> type) {
      super(fury, type);
      serializer = new ObjectSerializer<>(fury, Foo.class);
    }

    @Override
    public void write(MemoryBuffer buffer, Foo value) {
      final Map<String, UserContextResolver> userContextResolvers =
          fury.getSerializationContext().getUserContextResolvers();
      for (int i = 0; i < NUM; i++) {
        final StringUserContextResolver userContextResolver =
            (StringUserContextResolver) userContextResolvers.get(String.valueOf(i));
        userContextResolver.data = getData(i);
      }
      serializer.write(buffer, value);
    }

    @Override
    public Foo read(MemoryBuffer buffer) {
      final Map<String, UserContextResolver> userContextResolvers =
          fury.getSerializationContext().getUserContextResolvers();
      for (int i = 0; i < NUM; i++) {
        final StringUserContextResolver userContextResolver =
            (StringUserContextResolver) userContextResolvers.get(String.valueOf(i));
        Assert.assertEquals(userContextResolver.data, getData(i));
      }
      return serializer.read(buffer);
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
    Fury fury = buildFury();
    fury.registerSerializer(Foo.class, FooSerializer.class);
    List<StringUserContextResolver> resolvers = new ArrayList<>(NUM);
    for (int i = 0; i < NUM; i++) {
      final StringUserContextResolver userContextResolver = new StringUserContextResolver(fury);
      fury.registerUserContext(String.valueOf(i), StringUserContextResolver::new);
      resolvers.add(userContextResolver);
    }
    final Foo o = Foo.create();
    final byte[] bytes = fury.serialize(o);
    for (int i = 0; i < NUM; i++) {
      Assert.assertNull(resolvers.get(i).data);
    }
    Assert.assertEquals(fury.deserialize(bytes), o);
    for (int i = 0; i < NUM; i++) {
      Assert.assertNull(resolvers.get(i).data);
    }
  }

  private List<StringUserContextResolver> getResolver(Fury fury, int num, boolean empty) {
    return IntStream.range(0, num)
        .mapToObj(
            idx -> {
              final StringUserContextResolver userContextResolver =
                  new StringUserContextResolver(fury);
              if (!empty) {
                userContextResolver.data = getData(idx);
              }
              return userContextResolver;
            })
        .collect(Collectors.toList());
  }

  private static String getData(int idx) {
    return "data" + idx;
  }
}
