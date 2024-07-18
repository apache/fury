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

package org.apache.fury.benchmark;

import com.google.protobuf.InvalidProtocolBufferException;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import org.apache.fury.Fury;
import org.apache.fury.config.CompatibleMode;
import org.apache.fury.integration_tests.state.generated.ProtoMessage;
import org.openjdk.jmh.Main;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.CompilerControl;
import org.openjdk.jmh.annotations.Mode;

@BenchmarkMode(Mode.Throughput)
@CompilerControl(value = CompilerControl.Mode.INLINE)
public class StructBenchmark {
  private static NumericStructList structList;
  private static byte[] furyBytes;
  private static byte[] furyStrictBytes;
  private static byte[] furyKVCompatibleBytes;
  private static byte[] pbBytes;

  private static final Fury fury =
      Fury.builder()
          .withCompatibleMode(CompatibleMode.COMPATIBLE)
          .withScopedMetaShare(true)
          .build();

  private static final Fury furyStrict =
      Fury.builder().withCompatibleMode(CompatibleMode.SCHEMA_CONSISTENT).build();

  private static final Fury furyKVCompatible =
      Fury.builder()
          .withCompatibleMode(CompatibleMode.COMPATIBLE)
          .withScopedMetaShare(false)
          .build();

  static {
    try {
      fury.register(NumericStruct.class);
      fury.register(NumericStructList.class);
      furyStrict.register(NumericStruct.class);
      furyStrict.register(NumericStructList.class);
      furyKVCompatible.register(NumericStruct.class);
      furyKVCompatible.register(NumericStructList.class);
      structList = NumericStructList.build();
      furyBytes = fury.serialize(structList);
      furyStrictBytes = furyStrict.serialize(structList);
      furyKVCompatibleBytes = furyKVCompatible.serialize(structList);
      pbBytes = NumericStructList.buildPBStruct(structList).toByteArray();
      System.out.println("Fury serialized size: " + furyBytes.length);
      System.out.println("FuryStrict serialized size: " + furyStrictBytes.length);
      System.out.println("FuryKVCompatible serialized size: " + furyKVCompatibleBytes.length);
      System.out.println("PB serialized size: " + pbBytes.length);
    } catch (Exception e) {
      e.printStackTrace();
      throw new RuntimeException(e);
    }
  }

  @Benchmark
  public Object protobuf_serialize() {
    return NumericStructList.buildPBStruct(structList).toByteArray();
  }

  @Benchmark
  public Object protobuf_deserialize() {
    return NumericStructList.fromPBBytes(pbBytes);
  }

  @Benchmark
  public Object fury_serialize() {
    return fury.serialize(structList);
  }

  @Benchmark
  public Object fury_deserialize() {
    return fury.deserialize(furyBytes);
  }

  @Benchmark
  public Object furystrict_serialize() {
    return furyStrict.serialize(structList);
  }

  @Benchmark
  public Object furystrict_deserialize() {
    return furyStrict.deserialize(furyStrictBytes);
  }

  @Benchmark
  public Object furykv_serialize() {
    return furyKVCompatible.serialize(structList);
  }

  @Benchmark
  public Object furykv_deserialize() {
    return furyKVCompatible.deserialize(furyKVCompatibleBytes);
  }

  public static void main(String[] args) throws IOException {
    if (args.length == 0) {
      String commandLine =
          "org.apache.fury.*StructBenchmark.* -f 1 -wi 10 -i 10 -t 1 -w 2s -r 2s -rf csv";
      System.out.println(commandLine);
      args = commandLine.split(" ");
    }
    Main.main(args);
  }

  public static class NumericStructList {
    public List<NumericStruct> list;

    public static NumericStructList build() {
      NumericStructList structList = new NumericStructList();
      structList.list = new ArrayList<>(1000);
      for (int i = 0; i < 1000; i++) {
        structList.list.add(NumericStruct.build());
      }
      return structList;
    }

    public static ProtoMessage.StructList buildPBStruct(NumericStructList struct) {
      ProtoMessage.StructList.Builder builder = ProtoMessage.StructList.newBuilder();
      for (NumericStruct numericStruct : struct.list) {
        builder.addStructList(NumericStruct.buildPBStruct(numericStruct));
      }
      return builder.build();
    }

    public static NumericStructList fromPBBytes(byte[] pbBytes) {
      try {
        ProtoMessage.StructList pbList = ProtoMessage.StructList.parseFrom(pbBytes);
        NumericStructList structList = new NumericStructList();
        structList.list = new ArrayList<>();
        for (ProtoMessage.Struct struct : pbList.getStructListList()) {
          structList.list.add(NumericStruct.fromPBObject(struct));
        }
        return structList;
      } catch (InvalidProtocolBufferException e) {
        throw new RuntimeException(e);
      }
    }
  }

  public static class NumericStruct {
    public int f1;
    public int f2;
    public int f3;
    public int f4;
    public int f5;
    public int f6;
    public int f7;
    public int f8;

    public static NumericStruct build() {
      NumericStruct struct = new NumericStruct();
      struct.f1 = 1;
      struct.f2 = 2;
      struct.f3 = 3;
      struct.f4 = 4;
      struct.f5 = 5;
      struct.f6 = 6;
      struct.f7 = 7;
      struct.f8 = 8;
      return struct;
    }

    public static ProtoMessage.Struct buildPBStruct(NumericStruct struct) {
      ProtoMessage.Struct.Builder builder = ProtoMessage.Struct.newBuilder();
      builder.setF1(struct.f1);
      builder.setF2(struct.f2);
      builder.setF3(struct.f3);
      builder.setF4(struct.f4);
      builder.setF5(struct.f5);
      builder.setF6(struct.f6);
      builder.setF7(struct.f7);
      builder.setF8(struct.f8);
      return builder.build();
    }

    public static NumericStruct fromPBObject(ProtoMessage.Struct pbObject) {
      NumericStruct struct = new NumericStruct();
      struct.f1 = pbObject.getF1();
      struct.f2 = pbObject.getF2();
      struct.f3 = pbObject.getF3();
      struct.f4 = pbObject.getF4();
      struct.f5 = pbObject.getF5();
      struct.f6 = pbObject.getF6();
      struct.f7 = pbObject.getF7();
      struct.f8 = pbObject.getF8();
      return struct;
    }
  }
}
