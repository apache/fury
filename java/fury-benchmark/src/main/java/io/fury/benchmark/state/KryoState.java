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

package io.fury.benchmark.state;

import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.io.Input;
import com.esotericsoftware.kryo.io.Output;
import com.esotericsoftware.kryo.io.UnsafeMemoryInput;
import com.esotericsoftware.kryo.io.UnsafeMemoryOutput;
import com.esotericsoftware.kryo.serializers.CompatibleFieldSerializer;
import com.esotericsoftware.kryo.serializers.FieldSerializer;
import com.google.common.base.Preconditions;
import io.fury.benchmark.IntsSerializationSuite;
import io.fury.benchmark.LongStringSerializationSuite;
import io.fury.benchmark.LongsSerializationSuite;
import io.fury.benchmark.StringSerializationSuite;
import io.fury.benchmark.data.Data;
import io.fury.benchmark.data.Image;
import io.fury.benchmark.data.Media;
import io.fury.benchmark.data.MediaContent;
import io.fury.benchmark.data.Sample;
import io.fury.benchmark.data.Struct;
import java.util.ArrayList;
import org.openjdk.jmh.annotations.CompilerControl;
import org.openjdk.jmh.annotations.Fork;
import org.openjdk.jmh.annotations.Level;
import org.openjdk.jmh.annotations.Measurement;
import org.openjdk.jmh.annotations.Param;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.Warmup;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@Warmup(iterations = 3)
@Measurement(iterations = 3)
@Fork(value = 0)
@CompilerControl(value = CompilerControl.Mode.INLINE)
public class KryoState {
  private static final Logger LOG = LoggerFactory.getLogger(KryoState.class);

  public static void main(String[] args) {
    KryoUserTypeState state = new KryoUserTypeState();
    state.objectType = ObjectType.SAMPLE;
    state.bufferType = BufferType.directBuffer;
    state.setup();
  }

  @State(Scope.Thread)
  public abstract static class KryoBenchmarkState extends BenchmarkState {
    public Kryo kryo;
    public Output output;
    public Input input;

    @Setup(Level.Trial)
    public void setup() {
      kryo = new Kryo();
      switch (bufferType) {
        case array:
          output = new Output(1024 * 512);
          input = new Input(output.getBuffer());
          break;
        case directBuffer:
          output = new UnsafeMemoryOutput(1024 * 512);
          input = new UnsafeMemoryInput(((UnsafeMemoryOutput) output).getByteBuffer());
          break;
      }

      kryo.setReferences(references);
    }
  }

  public static class KryoUserTypeState extends KryoBenchmarkState {
    @Param() public ObjectType objectType;

    public Object object;
    public int serializedLength;

    @Override
    public void setup() {
      super.setup();
      if (compatible()) {
        kryo = new Kryo();
        kryo.setReferences(references);
        kryo.setDefaultSerializer(CompatibleFieldSerializer.class);
      }

      switch (objectType) {
        case SAMPLE:
          object = new Sample().populate(references);
          if (registerClass) {
            kryo.register(double[].class);
            kryo.register(int[].class);
            kryo.register(long[].class);
            kryo.register(float[].class);
            kryo.register(double[].class);
            kryo.register(short[].class);
            kryo.register(char[].class);
            kryo.register(boolean[].class);
            kryo.register(object.getClass());
          }
          break;
        case MEDIA_CONTENT:
          object = new MediaContent().populate(references);
          if (registerClass) {
            kryo.register(Image.class);
            kryo.register(Image.Size.class);
            kryo.register(Media.class);
            kryo.register(Media.Player.class);
            kryo.register(ArrayList.class);
            kryo.register(MediaContent.class);
          }
          break;
        case STRUCT:
          object = Struct.create(false);
          if (registerClass) {
            kryo.register(object.getClass());
          }
          break;
        case STRUCT2:
          object = Struct.create(true);
          if (registerClass) {
            kryo.register(object.getClass());
          }
          break;
      }
      kryo.setDefaultSerializer(FieldSerializer.class);

      output.setPosition(0);
      kryo.writeClassAndObject(output, object);
      serializedLength = output.position();
      LOG.info(
          "======> Kryo | {} | {} | {} | {} |",
          objectType,
          references,
          bufferType,
          serializedLength);
      input.setPosition(0);
      input.setLimit(serializedLength);
      Preconditions.checkArgument(object.equals(kryo.readClassAndObject(input)));
    }

    public boolean compatible() {
      return false;
    }
  }

  public static class KryoCompatibleState extends KryoUserTypeState {

    @Override
    public void setup() {
      super.setup();
      if (objectType == ObjectType.STRUCT) {
        Thread.currentThread()
            .setContextClassLoader(Struct.createStructClass(110, false).getClassLoader());
      }
    }

    @Override
    public boolean compatible() {
      return true;
    }
  }

  public static class DataState extends KryoBenchmarkState {
    public Data data = new Data();
  }

  public static class ReadIntsState extends DataState {
    @Override
    public void setup() {
      super.setup();
      new IntsSerializationSuite().kryo_serializeInts(this);
    }
  }

  public static class ReadLongsState extends DataState {
    @Override
    public void setup() {
      super.setup();
      new LongsSerializationSuite().kryo_serializeLongs(this);
    }
  }

  public static class ReadStrState extends DataState {
    @Override
    public void setup() {
      super.setup();
      new StringSerializationSuite().kryo_serializeStr(this);
    }
  }

  public static class ReadLongStrState extends DataState {
    @Override
    public void setup() {
      super.setup();
      new LongStringSerializationSuite().kryo_serializeLongStr(this);
    }
  }
}
