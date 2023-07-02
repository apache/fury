package io.fury.integration_tests;

import io.fury.benchmark.data.MediaContent;
import io.fury.benchmark.data.Sample;
import io.fury.benchmark.state.BufferType;
import io.fury.benchmark.state.ObjectType;
import io.fury.integration_tests.state.FlatBuffersState;
import io.fury.integration_tests.state.FlatBuffersState.FlatBuffersUserTypeState;
import io.fury.integration_tests.state.ProtoBuffersState;
import io.fury.integration_tests.state.ProtoBuffersState.ProtoBuffersUserTypeState;
import java.io.IOException;

import io.fury.util.Platform;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.CompilerControl;
import org.openjdk.jmh.annotations.Mode;

@BenchmarkMode(Mode.Throughput)
@CompilerControl(value = CompilerControl.Mode.INLINE)
public class UserTypeSerializeSuite extends io.fury.benchmark.UserTypeSerializeSuite {
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
