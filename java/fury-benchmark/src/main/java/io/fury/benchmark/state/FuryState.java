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

import com.google.common.base.Preconditions;
import io.fury.Fury;
import io.fury.Language;
import io.fury.benchmark.IntsSerializationSuite;
import io.fury.benchmark.LongStringSerializationSuite;
import io.fury.benchmark.LongsSerializationSuite;
import io.fury.benchmark.StringSerializationSuite;
import io.fury.benchmark.UserTypeDeserializeSuite;
import io.fury.benchmark.UserTypeSerializeSuite;
import io.fury.benchmark.data.Data;
import io.fury.benchmark.data.Image;
import io.fury.benchmark.data.Media;
import io.fury.benchmark.data.MediaContent;
import io.fury.benchmark.data.Struct;
import io.fury.memory.MemoryBuffer;
import io.fury.memory.MemoryUtils;
import io.fury.resolver.MetaContext;
import io.fury.serializer.CompatibleMode;
import io.fury.util.LoggerFactory;
import java.nio.ByteBuffer;
import java.util.ArrayList;
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
import org.slf4j.Logger;

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
              .ignoreStringReference(true) // for compare with fastjson
              .withReferenceTracking(references)
              .disableSecureMode()
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
            .withReferenceTracking(true)
            .disableSecureMode()
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
      Fury.FuryBuilder furyBuilder =
          Fury.builder()
              .withLanguage(Language.JAVA)
              .withClassVersionCheck(false)
              .ignoreStringReference(true) // for compare with fastjson
              .withReferenceTracking(references)
              .disableSecureMode();
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

      fury.writeReferencableToJava(buffer, object);
      serializedLength = buffer.writerIndex();
      LOG.info(
          "======> Fury | {} | {} | {} | {} |",
          objectType,
          references,
          bufferType,
          serializedLength);
      buffer.writerIndex(0);
      Preconditions.checkArgument(object.equals(fury.readReferencableFromJava(buffer)));
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
              .ignoreStringReference(true) // for compare with fastjson
              .withReferenceTracking(references)
              .disableSecureMode()
              .withMetaContextShareEnabled(true)
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
