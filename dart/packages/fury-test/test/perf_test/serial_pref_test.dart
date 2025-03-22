// @Skip()
library;

import 'dart:typed_data';

import 'package:fury_core/fury_core.dart';
import 'package:fury_test/entity/complex_obj_1.dart';
import 'package:fury_test/entity/complex_obj_2.dart';
import 'package:test/test.dart';

void _testPrefSer(Fury fury, Object? obj, ClassSpec spec, int times, String testName){
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

void _testPrefDeser(Fury fury, Object? obj, ClassSpec spec, int times, String testName){
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
  group('A group of tests', () {

    test('test serialize simple struct perf', () {
      Fury fury = Fury(
        xlangMode: true,
        refTracking: true,
      );
      fury.register($ComplexObject2, "test.ComplexObject2");
      ComplexObject2 o = ComplexObject2(true,{Int8(-1):Int32(2)});
      _testPrefSer(fury, o, $ComplexObject2, 1000000, 'test serialize simple struct perf');
    });

    test('test deserialize simple struct perf', () {
      Fury fury = Fury(
        xlangMode: true,
        refTracking: true,
      );
      fury.register($ComplexObject2, "test.ComplexObject2");
      ComplexObject2 o = ComplexObject2(true,{Int8(-1):Int32(2)});
      _testPrefDeser(fury, o, $ComplexObject2, 1000000, 'test deserialize simple struct perf');
    });

    test('test serialize medium complex struct perf', () {
      Fury fury = Fury(
        xlangMode: true,
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
      _testPrefSer(fury, obj, $ComplexObject1, 1000000, 'test deserialize medium complex struct perf');
    });

    test('test deserialize simple struct perf', () {
      Fury fury = Fury(
        xlangMode: true,
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
      _testPrefDeser(fury, obj, $ComplexObject1, 1000000, 'test serialize medium complex struct perf');
    });
  });
}