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

package org.apache.fory.integration_tests;

import java.time.Instant;
import java.time.LocalDate;
import org.apache.fory.format.encoder.Encoders;
import org.apache.fory.format.encoder.RowEncoder;
import org.apache.fory.format.row.binary.BinaryRow;
import org.apache.fory.memory.MemoryBuffer;
import org.apache.fory.memory.MemoryUtils;
import org.testng.Assert;
import org.testng.annotations.Test;

public class RecordRowTest {

  public record TestRecord(Instant f1, String f2, LocalDate f3) {}

  // Intentionally mis-ordered to ensure record component order is different from sorted field order
  public record OuterTestRecord(long f2, long f1, TestRecord f3) {}

  @Test
  public void testRecord() {
    final TestRecord bean =
        new TestRecord(Instant.ofEpochMilli(42), "Luna", LocalDate.ofEpochDay(1234));
    final RowEncoder<TestRecord> encoder = Encoders.bean(TestRecord.class);
    final BinaryRow row = encoder.toRow(bean);
    final MemoryBuffer buffer = MemoryUtils.wrap(row.toBytes());
    row.pointTo(buffer, 0, buffer.size());
    final TestRecord deserializedBean = encoder.fromRow(row);
    Assert.assertEquals(deserializedBean, bean);
  }

  @Test
  public void testNestedRecord() {
    final TestRecord nested =
        new TestRecord(Instant.ofEpochMilli(43), "Mars", LocalDate.ofEpochDay(5678));
    final OuterTestRecord bean = new OuterTestRecord(12, 34, nested);
    final RowEncoder<OuterTestRecord> encoder = Encoders.bean(OuterTestRecord.class);
    final BinaryRow row = encoder.toRow(bean);
    final MemoryBuffer buffer = MemoryUtils.wrap(row.toBytes());
    row.pointTo(buffer, 0, buffer.size());
    final OuterTestRecord deserializedBean = encoder.fromRow(row);
    Assert.assertEquals(deserializedBean, bean);
  }

  public record TestRecordNestedInterface(NestedInterface f1) {}

  public interface NestedInterface {
    int f1();

    class Impl implements NestedInterface {
      @Override
      public int f1() {
        return 42;
      }
    }
  }

  @Test
  public void testRecordNestedInterface() {
    final TestRecordNestedInterface bean =
        new TestRecordNestedInterface(new NestedInterface.Impl());
    final RowEncoder<TestRecordNestedInterface> encoder =
        Encoders.bean(TestRecordNestedInterface.class);
    final BinaryRow row = encoder.toRow(bean);
    final MemoryBuffer buffer = MemoryUtils.wrap(row.toBytes());
    row.pointTo(buffer, 0, buffer.size());
    final TestRecordNestedInterface deserializedBean = encoder.fromRow(row);
    Assert.assertEquals(deserializedBean.f1().f1(), bean.f1().f1());
  }
}
