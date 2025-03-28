// @Skip()
library;

import 'dart:collection';
import 'dart:typed_data';
import 'package:fury/fury.dart';
import 'package:fury/fury_test.dart';
import 'package:test/test.dart';

class TestFuriable implements Furiable {
  @override
  Type get $furyType => String;
}

void testDeterminingDartType() {
  // prepare test data
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
    TestFuriable(),
  ];

  final resolver = DartTypeResolver.I;
  final iterations = 1000000; // 1 million iterations
  final stopwatch = Stopwatch();

  print('run each test $iterations times...\n');

  // Test getFuryType method
  stopwatch.start();
  for (int i = 0; i < iterations; i++) {
    resolver.getFuryType(testObjects[i % testObjects.length]);
  }
  stopwatch.stop();
  print('DartTypeResolver.getFuryType: ${stopwatch.elapsedMilliseconds} ms');

  // Reset the stopwatch
  stopwatch.reset();

  // Test runtimeType direct access
  stopwatch.start();
  for (int i = 0; i < iterations; i++) {
    testObjects[i % testObjects.length].runtimeType;
  }
  stopwatch.stop();
  print('directly obj.runtimeType: ${stopwatch.elapsedMilliseconds} ms');

  // Reset the stopwatch
  stopwatch.reset();

  // Test type check chain
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
