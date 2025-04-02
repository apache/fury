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

// @Skip()
library;

import 'dart:convert';
import 'dart:typed_data';
import 'package:test/test.dart';
import 'package:fury/fury.dart';
import 'package:fury_test/entity/complex_obj_1.dart';
import 'package:fury_test/entity/complex_obj_2.dart';

void _testPerfSer(Fury fury, Object? obj, int times, String testName){
  //warm up
  for (int i = 0; i < 10000; i++) {
    fury.toFury(obj);
  }
  // measure
  final stopwatch = Stopwatch()..start();
  for (int i = 0; i < times; ++i) {
    fury.toFury(obj);
  }
  stopwatch.stop();
  print('$testName\nserialize simple struct test $times times: ${stopwatch.elapsedMilliseconds} ms');
}

void _testPerfJson(Object? obj, int times, String testName){
  // warm up
  for (int i = 0; i < 10000; i++) {
    jsonEncode(obj);
  }
  // measure
  final stopwatch = Stopwatch()..start();
  for (int i = 0; i < times; ++i) {
    jsonEncode(obj);
  }
  stopwatch.stop();
  print('[Json Serialization]$testName\nserialize simple struct test $times times: ${stopwatch.elapsedMilliseconds} ms');
}

void _testPerfJsonDeser(Object? obj, int times) {
  String str = jsonEncode(obj);
  // warm up
  for (int i = 0; i < 10000; i++) {
    jsonDecode(str);
  }
  // measure
  final stopwatch = Stopwatch()..start();
  for (int i = 0; i < 10000; ++i) {
    jsonDecode(str);
  }
  stopwatch.stop();
  print('[Json Deserialization]deserialize simple struct test $times times: ${stopwatch.elapsedMilliseconds} ms');
}

void _testPerfDeser(Fury fury, Object? obj,  int times, String testName){
  Uint8List bytes = fury.toFury(obj);
  // warm up
  for (int i = 0; i < 10000; i++) {
    fury.fromFury(bytes);
  }
  // measure
  final stopwatch = Stopwatch()..start();
  for (int i = 0; i < times; i++) {
    fury.fromFury(bytes);
  }
  stopwatch.stop();
  print('$testName\ndeserialize simple struct test $times times: ${stopwatch.elapsedMilliseconds} ms');
}

void main() {
  group('Test Performance of Serialization and Deserialization', () {

    test('test serialize simple struct perf', () {
      Fury fury = Fury(
        refTracking: true,
      );
      fury.register($ComplexObject2, "test.ComplexObject2");
      ComplexObject2 o = ComplexObject2(true,{Int8(-1):Int32(2)});
      _testPerfSer(fury, o, 1000000, 'test serialize simple struct perf');
    });

    test('test deserialize simple struct perf', () {
      Fury fury = Fury(
        refTracking: true,
      );
      fury.register($ComplexObject2, "test.ComplexObject2");
      ComplexObject2 o = ComplexObject2(true,{Int8(-1):Int32(2)});
      _testPerfDeser(fury, o, 1000000, 'test deserialize simple struct perf');
    });

    test('test serialize medium complex struct perf', () {
      Fury fury = Fury(
        refTracking: true,
      );
      fury.register($ComplexObject2, "test.ComplexObject2");
      fury.register($ComplexObject1, "test.ComplexObject1");
      ComplexObject2 obj2 = ComplexObject2(true,{Int8(-1):Int32(2)});
      ComplexObject1 obj = ComplexObject1();
      obj.f1 = obj2;
      obj.f2 = "abc";
      obj.f3 = ["abc", "abc"];
      obj.f4 = {Int8(1): Int32(2)};
      obj.f5 = Int8.maxValue;
      obj.f6 = Int16.maxValue;
      obj.f7 = Int32.maxValue;
      obj.f8 = 0x7FFFFFFFFFFFFFFF;
      obj.f9 = Float32(1.0 / 2);
      obj.f10 = 1 / 3.0;
      obj.f11 = Int16List.fromList([1, 2]);
      obj.f12 = [Int16(-1),Int16(4)];
      _testPerfSer(fury, obj, 1000000, 'test deserialize medium complex struct perf');
    });

    test('test deserialize medium complex struct perf', () {
      Fury fury = Fury(
        refTracking: true,
      );
      fury.register($ComplexObject2, "test.ComplexObject2");
      fury.register($ComplexObject1, "test.ComplexObject1");
      ComplexObject2 obj2 = ComplexObject2(true,{Int8(-1):Int32(2)});
      ComplexObject1 obj = ComplexObject1();
      obj.f1 = obj2;
      obj.f2 = "abc";
      obj.f3 = ["abc", "abc"];
      obj.f4 = {Int8(1): Int32(2)};
      obj.f5 = Int8.maxValue;
      obj.f6 = Int16.maxValue;
      obj.f7 = Int32.maxValue;
      obj.f8 = 0x7FFFFFFFFFFFFFFF;
      obj.f9 = Float32(1.0 / 2);
      obj.f10 = 1 / 3.0;
      obj.f11 = Int16List.fromList([1, 2]);
      obj.f12 = [Int16(-1),Int16(4)];
      _testPerfDeser(fury, obj, 1000000, 'test serialize medium complex struct perf');
    });

    // test('test json serialize medium complex struct perf', () {
    //   Fury fury = Fury(
    //     refTracking: true,
    //   );
    //   fury.register($ComplexObject2, "test.ComplexObject2");
    //   fury.register($ComplexObject1, "test.ComplexObject1");
    //   ComplexObject2 obj2 = ComplexObject2(true,{Int8(-1):Int32(2)});
    //   ComplexObject1 obj = ComplexObject1();
    //   obj.f1 = obj2;
    //   obj.f2 = "abc";
    //   obj.f3 = ["abc", "abc"];
    //   obj.f4 = {Int8(1): Int32(2)};
    //   obj.f5 = Int8.maxValue;
    //   obj.f6 = Int16.maxValue;
    //   obj.f7 = Int32.maxValue;
    //   obj.f8 = 0x7FFFFFFFFFFFFFFF;
    //   obj.f9 = Float32(1.0 / 2);
    //   obj.f10 = 1 / 3.0;
    //   obj.f11 = Int16List.fromList([1, 2]);
    //   obj.f12 = [Int16(-1),Int16(4)];
    //   _testPerfSer(fury, obj, 1000000, 'test deserialize medium complex struct perf');
    // });
    //
    // test('test json deserialize medium complex struct perf', () {
    //   Fury fury = Fury(
    //     refTracking: true,
    //   );
    //   fury.register($ComplexObject2, "test.ComplexObject2");
    //   fury.register($ComplexObject1, "test.ComplexObject1");
    //   ComplexObject2 obj2 = ComplexObject2(true,{Int8(-1):Int32(2)});
    //   ComplexObject1 obj = ComplexObject1();
    //   obj.f1 = obj2;
    //   obj.f2 = "abc";
    //   obj.f3 = ["abc", "abc"];
    //   obj.f4 = {Int8(1): Int32(2)};
    //   obj.f5 = Int8.maxValue;
    //   obj.f6 = Int16.maxValue;
    //   obj.f7 = Int32.maxValue;
    //   obj.f8 = 0x7FFFFFFFFFFFFFFF;
    //   obj.f9 = Float32(1.0 / 2);
    //   obj.f10 = 1 / 3.0;
    //   obj.f11 = Int16List.fromList([1, 2]);
    //   obj.f12 = [Int16(-1),Int16(4)];
    //   _testPerfDeser(fury, obj, 1000000, 'test serialize medium complex struct perf');
    // });
  });
}