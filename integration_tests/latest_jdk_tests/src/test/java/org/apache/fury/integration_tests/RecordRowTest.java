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

package org.apache.fury.integration_tests;

import org.apache.fury.format.encoder.Encoders;
import org.apache.fury.format.encoder.RowEncoder;
import org.apache.fury.format.row.binary.BinaryRow;
import org.apache.fury.memory.MemoryBuffer;
import org.apache.fury.memory.MemoryUtils;
import org.testng.Assert;
import org.testng.annotations.Test;

public class RecordRowTest {

  public record TestRecord(int f1, String f2) {}

  // Intentionally mis-ordered to ensure record component order is different from sorted field order
  public record OuterTestRecord(long f2, long f1, TestRecord f3) {}

  @Test
  public void testRecord() {
    final TestRecord bean = new TestRecord(42, "Luna");
    final RowEncoder<TestRecord> encoder = Encoders.bean(TestRecord.class);
    final BinaryRow row = encoder.toRow(bean);
    final MemoryBuffer buffer = MemoryUtils.wrap(row.toBytes());
    row.pointTo(buffer, 0, buffer.size());
    final TestRecord deserializedBean = encoder.fromRow(row);
    Assert.assertEquals(deserializedBean, bean);
  }

  @Test
  public void testNestedRecord() {
    final TestRecord nested = new TestRecord(43, "Mars");
    final OuterTestRecord bean = new OuterTestRecord(12, 34, nested);
    final RowEncoder<OuterTestRecord> encoder = Encoders.bean(OuterTestRecord.class);
    final BinaryRow row = encoder.toRow(bean);
    final MemoryBuffer buffer = MemoryUtils.wrap(row.toBytes());
    row.pointTo(buffer, 0, buffer.size());
    final OuterTestRecord deserializedBean = encoder.fromRow(row);
    Assert.assertEquals(deserializedBean, bean);
  }
}
