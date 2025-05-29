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

package org.apache.fory.serializer;

import java.nio.ByteBuffer;
import org.apache.fory.Fory;
import org.apache.fory.ForyTestBase;
import org.apache.fory.memory.ByteBufferUtil;
import org.testng.annotations.Test;

public class BufferSerializersTest extends ForyTestBase {

  @Test
  public void testByteBuffer() {
    Fory fory = Fory.builder().build();
    ByteBuffer buffer1 = ByteBuffer.allocate(32);
    buffer1.putLong(1000L);
    ByteBufferUtil.rewind(buffer1);
    serDeCheck(fory, buffer1);
    ByteBuffer buffer2 = ByteBuffer.allocateDirect(32);
    buffer2.putDouble(1.0 / 3);
    ByteBufferUtil.rewind(buffer2);
    serDeCheck(fory, buffer2);
  }

  @Test(dataProvider = "foryCopyConfig")
  public void testByteBuffer(Fory fory) {
    ByteBuffer buffer1 = ByteBuffer.allocate(32);
    buffer1.putLong(1000L);
    ByteBufferUtil.rewind(buffer1);
    copyCheck(fory, buffer1);
    ByteBuffer buffer2 = ByteBuffer.allocateDirect(32);
    buffer2.putDouble(1.0 / 3);
    ByteBufferUtil.rewind(buffer2);
    copyCheck(fory, buffer2);
  }
}
