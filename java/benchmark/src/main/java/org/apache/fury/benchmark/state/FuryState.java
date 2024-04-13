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

package org.apache.fury.benchmark.state;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import org.apache.fury.Fury;
import org.apache.fury.benchmark.IntsSerializationSuite;
import org.apache.fury.benchmark.LongStringSerializationSuite;
import org.apache.fury.benchmark.LongsSerializationSuite;
import org.apache.fury.benchmark.StringSerializationSuite;
import org.apache.fury.benchmark.UserTypeDeserializeSuite;
import org.apache.fury.benchmark.UserTypeSerializeSuite;
import org.apache.fury.benchmark.data.Data;
import org.apache.fury.benchmark.data.Image;
import org.apache.fury.benchmark.data.Media;
import org.apache.fury.benchmark.data.MediaContent;
import org.apache.fury.benchmark.data.Struct;
import org.apache.fury.config.CompatibleMode;
import org.apache.fury.config.FuryBuilder;
import org.apache.fury.config.Language;
import org.apache.fury.logging.Logger;
import org.apache.fury.logging.LoggerFactory;
import org.apache.fury.memory.MemoryBuffer;
import org.apache.fury.memory.MemoryUtils;
import org.apache.fury.resolver.MetaContext;
import org.apache.fury.util.Preconditions;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.CompilerControl;
import org.openjdk.jmh.annotations.Fork;
import org.openjdk.jmh.annotations.Level;
import org.openjdk.jmh.annotations.Measurement;
import org.openjdk.jmh.annotations.Mode;
import org.openjdk.jmh.annotations.Param;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.Warmup;

@BenchmarkMode(Mode.Throughput)
@Warmup(iterations = 3)
@Measurement(iterations = 3)
@Fork(value = 0)
@CompilerControl(value = CompilerControl.Mode.INLINE)
public class FuryState {
  private static final Logger LOG = LoggerFactory.getLogger(FuryState.class);

  @State(Scope.Thread)
  public abstract static class FuryBenchmarkState extends BenchmarkState {
    public Fury fury;
    public MemoryBuffer buffer;

    @Setup(Level.Trial)
    public void setup() {
      fury =
          Fury.builder()
              .withLanguage(Language.JAVA)
              .withClassVersionCheck(false)
              .withRefTracking(references)
              .requireClassRegistration(false)
              .build();
      setupBuffer();
    }

    public void setupBuffer() {
      switch (bufferType) {
        case array:
          buffer = MemoryUtils.buffer(1024 * 512);
          break;
        case directBuffer:
          buffer = MemoryUtils.wrap(ByteBuffer.allocateDirect(1024 * 512));
          break;
      }
    }
  }

  public static void main(String[] args) {
    Object o = Struct.create(true);
    Fury fury =
        Fury.builder()
            .withLanguage(Language.JAVA)
            .withRefTracking(true)
            .requireClassRegistration(false)
            .build();
    Object o1 = fury.deserialize(fury.serialize(o));
    Preconditions.checkArgument(o.equals(o1));
    FuryUserTypeState state = new FuryUserTypeState();
    state.objectType = ObjectType.MEDIA_CONTENT;
    state.bufferType = BufferType.directBuffer;
    state.setup();
    fury.serialize(new MediaContent().populate(false));
    fury.serialize(Struct.create(true));
    fury.serialize(Struct.create(false));
  }

  public static class FuryUserTypeState extends FuryBenchmarkState {
    @Param() public ObjectType objectType;

    public Object object;
    public int serializedLength;

    @Override
    public void setup() {
      setupBuffer();
      object = ObjectType.createObject(objectType, references);
      Thread.currentThread().setContextClassLoader(object.getClass().getClassLoader());
      FuryBuilder furyBuilder =
          Fury.builder()
              .withLanguage(Language.JAVA)
              .withClassVersionCheck(false)
              .withRefTracking(references)
              .requireClassRegistration(false);
      if (compatible()) {
        furyBuilder.withCompatibleMode(CompatibleMode.COMPATIBLE);
      }
      fury = furyBuilder.build();
      switch (objectType) {
        case SAMPLE:
        case STRUCT:
        case STRUCT2:
          if (registerClass) {
            fury.register(object.getClass());
          }
          break;
        case MEDIA_CONTENT:
          if (registerClass) {
            fury.register(Image.class);
            fury.register(Image.Size.class);
            fury.register(Media.class);
            fury.register(Media.Player.class);
            fury.register(ArrayList.class);
            fury.register(MediaContent.class);
            fury.register(Struct.class);
          }
          break;
      }

      fury.writeRef(buffer, object);
      serializedLength = buffer.writerIndex();
      LOG.info(
          "======> Fury | {} | {} | {} | {} |",
          objectType,
          references,
          bufferType,
          serializedLength);
      buffer.writerIndex(0);
      Preconditions.checkArgument(object.equals(fury.readRef(buffer)));
      buffer.readerIndex(0);
    }

    public boolean compatible() {
      return false;
    }
  }

  public static class FuryCompatibleState extends FuryUserTypeState {
    @Override
    public boolean compatible() {
      return true;
    }
  }

  public static class FuryMetaSharedState extends FuryUserTypeState {
    public MetaContext writerMetaContext = new MetaContext();
    public MetaContext readerMetaContext = new MetaContext();

    @Override
    public void setup() {
      setupBuffer();
      object = ObjectType.createObject(objectType, references);
      Thread.currentThread().setContextClassLoader(object.getClass().getClassLoader());
      fury =
          Fury.builder()
              .withLanguage(Language.JAVA)
              .withClassVersionCheck(false)
              .withRefTracking(references)
              .requireClassRegistration(false)
              .withMetaContextShare(true)
              .withCompatibleMode(CompatibleMode.COMPATIBLE)
              .build();
      // share meta first time.
      new UserTypeSerializeSuite().furymetashared_serialize_compatible(this);
      new UserTypeDeserializeSuite().furymetashared_deserialize_compatible(this);
      // meta shared.
      new UserTypeSerializeSuite().furymetashared_serialize_compatible(this);
      serializedLength = buffer.writerIndex();
      LOG.info(
          "======> Fury metashared | {} | {} | {} | {} |",
          objectType,
          references,
          bufferType,
          serializedLength);
      buffer.writerIndex(0);
      Object o = new UserTypeDeserializeSuite().furymetashared_deserialize_compatible(this);
      Preconditions.checkArgument(object.equals(o));
      buffer.readerIndex(0);
    }

    @Override
    public boolean compatible() {
      return true;
    }
  }

  public static class DataState extends FuryBenchmarkState {
    public Data data = new Data();
  }

  public static class ReadIntsState extends DataState {
    @Override
    public void setup() {
      super.setup();
      new IntsSerializationSuite().fury_serializeInts(this);
    }
  }

  public static class ReadLongsState extends DataState {
    @Override
    public void setup() {
      super.setup();
      new LongsSerializationSuite().fury_serializeLongs(this);
    }
  }

  public static class ReadStrState extends DataState {
    @Override
    public void setup() {
      super.setup();
      new StringSerializationSuite().fury_serializeStr(this);
    }
  }

  public static class ReadLongStrState extends DataState {
    @Override
    public void setup() {
      super.setup();
      new LongStringSerializationSuite().fury_serializeLongStr(this);
    }
  }
}
