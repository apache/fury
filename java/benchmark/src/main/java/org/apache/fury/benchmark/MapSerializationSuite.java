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

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import org.apache.fury.Fury;
import org.apache.fury.serializer.Serializer;
import org.apache.fury.serializer.collection.AbstractMapSerializer;
import org.openjdk.jmh.Main;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.CompilerControl;
import org.openjdk.jmh.annotations.Level;
import org.openjdk.jmh.annotations.Mode;
import org.openjdk.jmh.annotations.Param;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;

@BenchmarkMode(Mode.Throughput)
@CompilerControl(value = CompilerControl.Mode.INLINE)
public class MapSerializationSuite {
  public static void main(String[] args) throws IOException {
    if (args.length == 0) {
      String commandLine =
          "org.apache.fury.*MapSerializationSuite.* -f 1 -wi 3 -i 3 -t 1 -w 2s -r 2s -rf csv";
      System.out.println(commandLine);
      args = commandLine.split(" ");
    }
    Main.main(args);
  }

  @State(Scope.Thread)
  public static class MapState {
    @Param({"5", "20", "50", "100", "200"})
    public int mapSize;

    @Param({"false", "true"})
    public boolean enableChunkEncoding;

    private Map<String, String> stringMap;
    private Map<Integer, Integer> integerMap;
    private byte[] stringMapBytes;
    private byte[] integerMapBytes;
    private Fury fury;

    @Setup(Level.Trial)
    public void setup() {
      fury = Fury.builder().build();
      Serializer<HashMap> serializer = fury.getSerializer(HashMap.class);
      ((AbstractMapSerializer) serializer).setUseChunkSerialize(enableChunkEncoding);
      stringMap = new HashMap<>(mapSize);
      integerMap = new HashMap<>(mapSize);
      for (int i = 0; i < mapSize; i++) {
        stringMap.put("k" + i, "v" + i);
        integerMap.put(i, i * 2);
      }
      stringMapBytes = fury.serialize(stringMap);
      integerMapBytes = fury.serialize(integerMap);
    }
  }

  @Benchmark
  public Object serializeStringMap(MapState state) {
    return state.fury.serialize(state.stringMap);
  }

  @Benchmark
  public Object serializeIntMap(MapState state) {
    return state.fury.serialize(state.integerMap);
  }

  @Benchmark
  public Object deserializeStringMap(MapState state) {
    return state.fury.deserialize(state.stringMapBytes);
  }

  @Benchmark
  public Object deserializeIntMap(MapState state) {
    return state.fury.deserialize(state.integerMapBytes);
  }
}
