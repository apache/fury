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

package org.apache.fory.format.encoder;

import java.util.Optional;
import lombok.Data;
import org.apache.fory.annotation.ForyField;
import org.apache.fory.format.row.binary.BinaryRow;
import org.apache.fory.memory.MemoryBuffer;
import org.apache.fory.memory.MemoryUtils;
import org.testng.Assert;
import org.testng.annotations.Test;

public class ImplementInterfaceTest {

  public interface InterfaceType {
    int getF1();

    String getF2();

    NestedType getNested();

    PoisonPill getPoison();

    static PoisonPill builder() {
      return new PoisonPill();
    }
  }

  public interface NestedType {
    @ForyField(nullable = false)
    String getF3();
  }

  public static class PoisonPill {}

  @Data
  static class ImplementInterface implements InterfaceType {
    public int f1;
    public String f2;
    public NestedType nested;
    public PoisonPill poison = new PoisonPill();

    public ImplementInterface(final int f1, final String f2) {
      this.f1 = f1;
      this.f2 = f2;
    }
  }

  @Data
  static class ImplementNestedType implements NestedType {
    public String f3;

    public ImplementNestedType(final String f3) {
      this.f3 = f3;
    }
  }

  static {
    Encoders.registerCustomCodec(PoisonPill.class, new PoisonPillCodec());
    Encoders.registerCustomCodec(Id.class, new IdCodec());
  }

  @Test
  public void testInterfaceTypes() {
    final InterfaceType bean1 = new ImplementInterface(42, "42");
    final RowEncoder<InterfaceType> encoder = Encoders.bean(InterfaceType.class);
    final BinaryRow row = encoder.toRow(bean1);
    final MemoryBuffer buffer = MemoryUtils.wrap(row.toBytes());
    row.pointTo(buffer, 0, buffer.size());
    final InterfaceType deserializedBean = encoder.fromRow(row);
    assertEquals(bean1, deserializedBean);
  }

  @Test
  public void testNullValue() {
    final InterfaceType bean1 = new ImplementInterface(42, null);
    final RowEncoder<InterfaceType> encoder = Encoders.bean(InterfaceType.class);
    final BinaryRow row = encoder.toRow(bean1);
    final MemoryBuffer buffer = MemoryUtils.wrap(row.toBytes());
    row.pointTo(buffer, 0, buffer.size());
    final InterfaceType deserializedBean = encoder.fromRow(row);
    assertEquals(deserializedBean, bean1);
  }

  @Test
  public void testNestedValue() {
    final ImplementInterface bean1 = new ImplementInterface(42, "42");
    bean1.nested = new ImplementNestedType("f3");
    final RowEncoder<InterfaceType> encoder = Encoders.bean(InterfaceType.class);
    final BinaryRow row = encoder.toRow(bean1);
    final MemoryBuffer buffer = MemoryUtils.wrap(row.toBytes());
    row.pointTo(buffer, 0, buffer.size());
    final InterfaceType deserializedBean = encoder.fromRow(row);
    assertEquals(bean1, deserializedBean);
    Assert.assertEquals(bean1.nested.getF3(), deserializedBean.getNested().getF3());
  }

  private void assertEquals(final InterfaceType bean1, final InterfaceType deserializedBean) {
    Assert.assertNotSame(deserializedBean.getClass(), bean1.getClass());
    Assert.assertEquals(deserializedBean.getF1(), bean1.getF1());
    Assert.assertEquals(deserializedBean.getF2(), bean1.getF2());
  }

  static class PoisonPillCodec implements CustomCodec.ByteArrayCodec<PoisonPill> {
    @Override
    public byte[] encode(final PoisonPill value) {
      return new byte[0];
    }

    @Override
    public PoisonPill decode(final byte[] value) {
      throw new AssertionError();
    }
  }

  public interface OptionalType {
    Optional<String> f1();
  }

  static class OptionalTypeImpl implements OptionalType {
    private final Optional<String> f1;

    OptionalTypeImpl(final Optional<String> f1) {
      this.f1 = f1;
    }

    @Override
    public Optional<String> f1() {
      return f1;
    }
  }

  @Test
  public void testNullOptional() {
    final OptionalType bean1 = new OptionalTypeImpl(null);
    final RowEncoder<OptionalType> encoder = Encoders.bean(OptionalType.class);
    final BinaryRow row = encoder.toRow(bean1);
    final MemoryBuffer buffer = MemoryUtils.wrap(row.toBytes());
    row.pointTo(buffer, 0, buffer.size());
    final OptionalType deserializedBean = encoder.fromRow(row);
    Assert.assertEquals(deserializedBean.f1(), Optional.empty());
  }

  @Test
  public void testPresentOptional() {
    final OptionalType bean1 = new OptionalTypeImpl(Optional.of("42"));
    final RowEncoder<OptionalType> encoder = Encoders.bean(OptionalType.class);
    final BinaryRow row = encoder.toRow(bean1);
    final MemoryBuffer buffer = MemoryUtils.wrap(row.toBytes());
    row.pointTo(buffer, 0, buffer.size());
    final OptionalType deserializedBean = encoder.fromRow(row);
    Assert.assertEquals(deserializedBean.f1(), Optional.of("42"));
  }

  public static class Id<T> {
    byte id;

    Id(final byte id) {
      this.id = id;
    }
  }

  public interface OptionalCustomType {
    Optional<Id<OptionalCustomType>> f1();
  }

  static class OptionalCustomTypeImpl implements OptionalCustomType {
    @Override
    public Optional<Id<OptionalCustomType>> f1() {
      return Optional.of(new Id<>((byte) 42));
    }
  }

  static class IdCodec<T> implements CustomCodec.MemoryBufferCodec<Id<T>> {
    @Override
    public MemoryBuffer encode(final Id<T> value) {
      return MemoryBuffer.fromByteArray(new byte[] {value.id});
    }

    @Override
    public Id<T> decode(final MemoryBuffer value) {
      return new Id<>(value.readByte());
    }
  }

  @Test
  public void testOptionalCustomType() {
    final OptionalCustomType bean1 = new OptionalCustomTypeImpl();
    final RowEncoder<OptionalCustomType> encoder = Encoders.bean(OptionalCustomType.class);
    final BinaryRow row = encoder.toRow(bean1);
    final MemoryBuffer buffer = MemoryUtils.wrap(row.toBytes());
    row.pointTo(buffer, 0, buffer.size());
    final OptionalCustomType deserializedBean = encoder.fromRow(row);
    Assert.assertEquals(deserializedBean.f1().get().id, bean1.f1().get().id);
  }
}
