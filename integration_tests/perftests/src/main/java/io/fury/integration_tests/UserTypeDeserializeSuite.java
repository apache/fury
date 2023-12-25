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

package io.fury.integration_tests;

import io.fury.benchmark.state.ObjectType;
import io.fury.integration_tests.state.FlatBuffersState;
import io.fury.integration_tests.state.FlatBuffersState.FlatBuffersUserTypeState;
import io.fury.integration_tests.state.ProtoBuffersState;
import io.fury.integration_tests.state.ProtoBuffersState.ProtoBuffersUserTypeState;
import io.fury.util.Platform;
import java.io.IOException;
import org.openjdk.jmh.Main;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.CompilerControl;
import org.openjdk.jmh.annotations.Mode;

@BenchmarkMode(Mode.Throughput)
@CompilerControl(value = CompilerControl.Mode.INLINE)
public class UserTypeDeserializeSuite extends io.fury.benchmark.UserTypeDeserializeSuite {

  @Benchmark
  public Object protobuffers_deserialize(ProtoBuffersUserTypeState state) {
    if (state.objectType == ObjectType.SAMPLE) {
      return ProtoBuffersState.deserializeSample(state.data);
    } else {
      return ProtoBuffersState.deserializeMediaContent(state.data);
    }
  }

  @Benchmark
  public Object flatbuffers_deserialize(FlatBuffersUserTypeState state) {
    Platform.clearBuffer(state.deserializedData);
    if (state.objectType == ObjectType.SAMPLE) {
      return FlatBuffersState.deserializeSample(state.deserializedData);
    } else {
      return FlatBuffersState.deserializeMediaContent(state.deserializedData);
    }
  }

  public static void main(String[] args) throws IOException {
    if (args.length == 0) {
      String commandLine =
          "io.*.integration_tests.UserTypeDeserializeSuite.*buffers.* -f 0 -wi 0 -i 1 -t 1 -w 2s -r 1s -rf csv";
      System.out.println(commandLine);
      args = commandLine.split(" ");
    }
    Main.main(args);
  }
}
