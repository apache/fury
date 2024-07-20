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
import org.apache.fury.benchmark.state.FuryState;
import org.apache.fury.benchmark.state.KryoState;
import org.openjdk.jmh.Main;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.CompilerControl;
import org.openjdk.jmh.annotations.Mode;

@BenchmarkMode(Mode.Throughput)
@CompilerControl(value = CompilerControl.Mode.INLINE)
public class CopyBenchmark {

  @Benchmark
  public Object fury_copy(FuryState.FuryUserTypeState state) {
    return state.fury.copy(state.object);
  }

  @Benchmark
  public Object kryo_copy(KryoState.KryoUserTypeState state) {
    return state.kryo.copy(state.object);
  }

  public static void main(String[] args) throws IOException {
    if (args.length == 0) {
      String commandLine =
          "org.apache.fury.*CopyBenchmark.* -f 1 -wi 3 -i 3 -t 1 -w 2s -r 2s -rf csv "
              + "-p bufferType=array -p references=false";
      System.out.println(commandLine);
      args = commandLine.split(" ");
    }
    Main.main(args);
  }
}
