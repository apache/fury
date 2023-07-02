/*
 * Copyright 2023 The Fury authors
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package io.fury.benchmark;

import io.fury.benchmark.state.FstState;
import io.fury.benchmark.state.FuryState;
import io.fury.benchmark.state.HessionState;
import io.fury.benchmark.state.JDKState;
import io.fury.benchmark.state.JsonbState;
import io.fury.benchmark.state.KryoState;
import io.fury.benchmark.state.ProtostuffState;
import java.io.IOException;
import org.openjdk.jmh.Main;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.CompilerControl;
import org.openjdk.jmh.annotations.Mode;
import org.openjdk.jmh.infra.Blackhole;

@BenchmarkMode(Mode.Throughput)
@CompilerControl(value = CompilerControl.Mode.INLINE)
public class UserTypeDeserializeSuite {

  @Benchmark
  public Object kryo_deserialize(KryoState.KryoUserTypeState state) {
    state.input.setPosition(0);
    state.input.setLimit(state.serializedLength);
    return state.kryo.readClassAndObject(state.input);
  }

  @Benchmark
  public Object kryo_deserialize_compatible(KryoState.KryoCompatibleState state) {
    state.input.setPosition(0);
    state.input.setLimit(state.serializedLength);
    return state.kryo.readClassAndObject(state.input);
  }

  @Benchmark
  public Object fury_deserialize(FuryState.FuryUserTypeState state) {
    state.buffer.readerIndex(0);
    Object o = state.fury.readReferencableFromJava(state.buffer);
    state.fury.resetRead();
    return o;
  }

  @Benchmark
  public Object fury_deserialize_compatible(FuryState.FuryCompatibleState state) {
    state.buffer.readerIndex(0);
    Object o = state.fury.readReferencableFromJava(state.buffer);
    state.fury.resetRead();
    return o;
  }

  @Benchmark
  public Object furymetashared_deserialize_compatible(FuryState.FuryMetaSharedState state) {
    state.buffer.readerIndex(0);
    state.fury.getSerializationContext().setMetaContext(state.metaContext);
    Object o = state.fury.readReferencableFromJava(state.buffer);
    state.fury.resetRead();
    return o;
  }

  @Benchmark
  public Object fst_deserialize(FstState.FstUserTypeState state, Blackhole bh) {
    return FstState.FstBenchmarkState.deserialize(bh, state);
  }

  @Benchmark
  public Object hession_deserialize(HessionState.HessionUserTypeState state) {
    state.bis.reset();
    state.input.reset();
    return HessionState.deserialize(state.input);
  }

  @Benchmark
  public Object hession_deserialize_compatible(HessionState.HessianCompatibleState state) {
    state.bis.reset();
    state.input.reset();
    return HessionState.deserialize(state.input);
  }

  @Benchmark
  public Object protostuff_deserialize(ProtostuffState.ProtostuffUserTypeState state) {
    return ProtostuffState.deserialize(state.schema, state.protoStuff);
  }

  @Benchmark
  public Object jdk_deserialize(JDKState.JDKUserTypeState state) {
    state.bis.reset();
    return JDKState.deserialize(state.bis);
  }

  @Benchmark
  public Object jsonb_deserialize(JsonbState.JsonbUserTypeState state, Blackhole bh) {
    return JsonbState.deserialize(bh, state);
  }

  public static void main(String[] args) throws IOException {
    if (args.length == 0) {
      String commandLine =
          "io.*UserTypeDeserializeSuite.* -f 3 -wi 3 -i 3 -t 1 -w 2s -r 2s -rf csv";
      System.out.println(commandLine);
      args = commandLine.split(" ");
    }
    Main.main(args);
  }
}
