// @Skip()
library;

import 'dart:collection';
import 'dart:typed_data';

import 'package:fury_core/fury_core.dart';
import 'package:fury_core/fury_core_test.dart';
import 'package:test/test.dart';

class TestFurable implements Furable {
  @override
  Type get $furyType => String;
}

void testDeterminingDartType() {
  // 准备测试数据
  final testObjects = [
    'string',
    123,
    3.14,
    true,
    <String, dynamic>{'key': 'value'},
    HashMap<String, dynamic>.from({'a': 1}),
    LinkedHashMap<String, dynamic>.from({'b': 2}),
    <dynamic>[1, 2, 3],
    Uint8List(10),
    Int8List(10),
    Float32List(10),
    HashSet<int>.from([1, 2, 3]),
    LinkedHashSet<int>.from([4, 5, 6]),
    TestFurable(),
  ];

  final resolver = DartTypeResolver.I;
  final iterations = 1000000; // 100万次迭代
  final stopwatch = Stopwatch();

  print('run each test $iterations times...\n');

  // 测试 getFuryType 方法
  stopwatch.start();
  for (int i = 0; i < iterations; i++) {
    resolver.getFuryType(testObjects[i % testObjects.length]);
  }
  stopwatch.stop();
  print('DartTypeResolver.getFuryType: ${stopwatch.elapsedMilliseconds} ms');

  // 重置计时器
  stopwatch.reset();

  // 测试 runtimeType 直接访问
  stopwatch.start();
  for (int i = 0; i < iterations; i++) {
    testObjects[i % testObjects.length].runtimeType;
  }
  stopwatch.stop();
  print('directly obj.runtimeType: ${stopwatch.elapsedMilliseconds} ms');

  // 重置计时器
  stopwatch.reset();

  // 测试类型检查链
  stopwatch.start();
  for (int i = 0; i < iterations; i++) {
    var obj = testObjects[i % testObjects.length];
    if (obj is Map) {
      if (obj is LinkedHashMap) {}
      else if (obj is HashMap) {}
    }
    else if (obj is List) {
      if (obj is Uint8List) {}
      else if (obj is Int8List) {}
    }
    else if (obj is Set) {
      if (obj is HashSet) {}
    }
  }
  stopwatch.stop();
  print('type chain: ${stopwatch.elapsedMilliseconds} ms');
}

void main(){
  test('testDeterminingDartType', () {
    testDeterminingDartType();
  });
}