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

package org.apache.fury.io;

import static org.testng.Assert.*;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import org.apache.fury.Fury;
import org.apache.fury.FuryTestBase;
import org.apache.fury.memory.MemoryBuffer;
import org.apache.fury.test.bean.Foo;
import org.testng.annotations.Test;

public class BlockedStreamUtilsTest extends FuryTestBase {

  @Test
  public void testDeserializeStream() {
    Fury fury = getJavaFury();
    ByteArrayOutputStream stream = new ByteArrayOutputStream();
    Foo foo = Foo.create();
    BlockedStreamUtils.serialize(fury, stream, foo);
    BlockedStreamUtils.serializeJavaObject(fury, stream, foo);
    ByteArrayInputStream inputStream = new ByteArrayInputStream(stream.toByteArray());
    assertEquals(BlockedStreamUtils.deserialize(fury, inputStream), foo);
    assertEquals(BlockedStreamUtils.deserializeJavaObject(fury, inputStream, Foo.class), foo);
  }

  @Test
  public void testDeserializeChannel() {
    Fury fury = builder().withCodegen(false).build();
    ByteArrayOutputStream stream = new ByteArrayOutputStream();
    Foo foo = Foo.create();
    BlockedStreamUtils.serialize(fury, stream, foo);
    BlockedStreamUtils.serializeJavaObject(fury, stream, foo);
    try (MemoryBufferReadableChannel channel =
        new MemoryBufferReadableChannel(MemoryBuffer.fromByteArray(stream.toByteArray()))) {
      assertEquals(BlockedStreamUtils.deserialize(fury, channel), foo);
      assertEquals(BlockedStreamUtils.deserializeJavaObject(fury, channel, Foo.class), foo);
    }
  }
}
