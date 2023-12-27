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

import java.io.IOException;
import org.apache.fury.benchmark.data.MediaContent;
import org.apache.fury.benchmark.data.Sample;
import org.apache.fury.benchmark.state.BufferType;
import org.apache.fury.benchmark.state.ObjectType;
import org.apache.fury.integration_tests.state.FlatBuffersState;
import org.apache.fury.integration_tests.state.FlatBuffersState.FlatBuffersUserTypeState;
import org.apache.fury.integration_tests.state.ProtoBuffersState;
import org.apache.fury.integration_tests.state.ProtoBuffersState.ProtoBuffersUserTypeState;
import org.apache.fury.util.Platform;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.CompilerControl;
import org.openjdk.jmh.annotations.Mode;

@BenchmarkMode(Mode.Throughput)
@CompilerControl(value = CompilerControl.Mode.INLINE)
public class UserTypeSerializeSuite extends org.apache.fury.benchmark.UserTypeSerializeSuite {
  @Benchmark
  public byte[] protobuffers_serialize(ProtoBuffersUserTypeState state) {
    if (state.objectType == ObjectType.SAMPLE) {
      return ProtoBuffersState.serializeSample((Sample) state.object);
    } else {
      return ProtoBuffersState.serializeMediaContent((MediaContent) state.object);
    }
  }

  @Benchmark
  public Object flatbuffers_serialize(FlatBuffersUserTypeState state) {
    Platform.clearBuffer(state.directBuffer);
    if (state.objectType == ObjectType.SAMPLE) {
      return FlatBuffersState.serializeSample((Sample) state.object, state.directBuffer);
    } else {
      return FlatBuffersState.serializeMediaContent((MediaContent) state.object);
    }
  }

  public static void main(String[] args) throws IOException {
    {
      ProtoBuffersUserTypeState state = new ProtoBuffersUserTypeState();
      state.objectType = ObjectType.SAMPLE;
      state.bufferType = BufferType.array;
      state.setup();
      if (true) {
        throw new RuntimeException();
      }
    }
    {
      FlatBuffersUserTypeState state = new FlatBuffersUserTypeState();
      state.objectType = ObjectType.SAMPLE;
      state.bufferType = BufferType.array;
      state.setup();
    }
    // if (args.length == 0) {
    //   String commandLine = "io.*.integration_tests.UserTypeSerializeSuite.*buffers.* -f 0 -wi 0
    // -i 1 -t 1 -w 1s -r 1s -rf csv";
    //   System.out.println(commandLine);
    //   args = commandLine.split(" ");
    // }
    // Main.main(args);
  }
}
