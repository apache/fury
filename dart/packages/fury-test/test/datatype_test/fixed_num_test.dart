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

import 'package:fury/fury.dart' show FixedNum, Float32, Int16, Int32, Int8;
import 'package:test/test.dart';

void main() {
  group('FixedNum Base Class Tests', () {
    test('FixedNum.from() creates the correct type', () {
      expect(FixedNum.from(42, type: 'int8'), isA<Int8>());
      expect(FixedNum.from(42, type: 'int16'), isA<Int16>());
      expect(FixedNum.from(42, type: 'int32'), isA<Int32>());
      expect(FixedNum.from(42, type: 'float32'), isA<Float32>());

      // Default type should be int32
      expect(FixedNum.from(42), isA<Int32>());
    });

    test('FixedNum.from() handles case insensitivity', () {
      expect(FixedNum.from(42, type: 'INT8'), isA<Int8>());
      expect(FixedNum.from(42, type: 'Int16'), isA<Int16>());
    });

    test('FixedNum.from() throws on invalid type', () {
      expect(() => FixedNum.from(42, type: 'invalid'), throwsArgumentError);
    });

    test('FixedNum instances are comparable', () {
      var a = FixedNum.from(10, type: 'int8');
      var b = FixedNum.from(20, type: 'int8');
      var c = FixedNum.from(10, type: 'int16');

      expect(a.compareTo(b) < 0, isTrue);
      expect(b.compareTo(a) > 0, isTrue);
      expect(a.compareTo(c) == 0, isTrue); // Different types but same value
    });
  });

  group('Int8 Tests', () {
    group('Construction and Range Handling', () {
      test('Constructor handles values within range', () {
        var a = Int8(127);
        var b = Int8(-128);

        expect(a.value, 127);
        expect(b.value, -128);
      });

      test('Constructor handles overflow and underflow', () {
        var a = Int8(128);  // Overflow
        var b = Int8(255);  // Overflow to -1
        var c = Int8(256);  // Overflow to 0
        var d = Int8(-129); // Underflow

        expect(a.value, -128);
        expect(b.value, -1);
        expect(c.value, 0);
        expect(d.value, 127);
      });

      test('Constructor handles floating point values', () {
        var a = Int8(42.7);
        var b = Int8(-42.7);

        expect(a.value, 42);
        expect(b.value, -42);
      });

      test('Min and Max values are correct', () {
        expect(Int8.MIN_VALUE, -128);
        expect(Int8.MAX_VALUE, 127);
        expect(Int8.minValue.value, -128);
        expect(Int8.maxValue.value, 127);
      });
    });

    group('Arithmetic Operators', () {
      test('Addition works with overflow handling', () {
        var a = Int8(100);
        var b = Int8(50);
        var result = a + b;

        expect(result, isA<Int8>());
        expect(result.value, -106); // 100 + 50 = 150, which overflows to -106
      });

      test('Addition works with FixedNum and raw numbers', () {
        var a = Int8(100);
        var b = Int8(50);

        expect((a + b).value, -106);
        expect((a + 50).value, -106);
      });

      test('Subtraction works with underflow handling', () {
        var a = Int8(-100);
        var b = Int8(50);
        var result = a - b;

        expect(result, isA<Int8>());
        expect(result.value, 106); // -100 - 50 = -150, which underflows to 106
      });

      test('Multiplication works with overflow handling', () {
        var a = Int8(20);
        var b = Int8(10);
        var result = a * b;

        expect(result, isA<Int8>());
        expect(result.value, -56); // 20 * 10 = 200, which overflows to -56
      });

      test('Division returns double', () {
        var a = Int8(100);
        var b = Int8(25);
        var result = a / b;

        expect(result, isA<double>());
        expect(result, 4.0);
      });

      test('Integer division works with overflow handling', () {
        var a = Int8(100);
        var b = Int8(3);
        var result = a ~/ b;

        expect(result, isA<Int8>());
        expect(result.value, 33);
      });

      test('Modulo works correctly', () {
        var a = Int8(100);
        var b = Int8(30);
        var result = a % b;

        expect(result, isA<Int8>());
        expect(result.value, 10);
      });

      test('Negation works with overflow handling', () {
        var a = Int8(127);
        var b = Int8(-128);

        expect((-a).value, -127);
        expect((-b).value, -128); // -(-128) = 128, which overflows to -128
      });
    });

    group('Bitwise Operators', () {
      test('Bitwise AND works correctly', () {
        var a = Int8(170); // 10101010 in binary
        var b = Int8(240); // 11110000 in binary
        var result = a & b;

        expect(result, isA<Int8>());
        expect(result.value, Int8(160)); // 10100000 in binary
      });

      test('Bitwise OR works correctly', () {
        var a = Int8(170); // 10101010 in binary
        var b = Int8(240); // 11110000 in binary
        var result = a | b;

        expect(result, isA<Int8>());
        expect(result.value, Int8(250)); // 11111010 in binary
      });

      test('Bitwise XOR works correctly', () {
        var a = Int8(170); // 10101010 in binary
        var b = Int8(240); // 11110000 in binary
        var result = a ^ b;

        expect(result, isA<Int8>());
        expect(result.value, 90); // 01011010 in binary
      });

      test('Bitwise NOT works correctly', () {
        var a = Int8(170); // 10101010 in binary
        var result = ~a;

        expect(result, isA<Int8>());
        expect(result.value, 85); // 01010101 in binary
      });

      test('Left shift works correctly', () {
        var a = Int8(10); // 00001010 in binary
        var result = a << 2;

        expect(result, isA<Int8>());
        expect(result.value, 40); // 00101000 in binary
      });

      test('Right shift works correctly', () {
        var a = Int8(-96); // 10100000 in binary
        var result = a >> 3;

        expect(result, isA<Int8>());
        expect(result.value, -12); // 11110100 in binary
      });
    });

    group('Comparison Operators', () {
      test('Less than works correctly', () {
        var a = Int8(10);
        var b = Int8(20);

        expect(a < b, isTrue);
        expect(b < a, isFalse);
        expect(a < 20, isTrue);
      });

      test('Less than or equal works correctly', () {
        var a = Int8(10);
        var b = Int8(20);
        var c = Int8(10);

        expect(a <= b, isTrue);
        expect(a <= c, isTrue);
        expect(b <= a, isFalse);
      });

      test('Greater than works correctly', () {
        var a = Int8(20);
        var b = Int8(10);

        expect(a > b, isTrue);
        expect(b > a, isFalse);
        expect(a > 10, isTrue);
      });

      test('Greater than or equal works correctly', () {
        var a = Int8(20);
        var b = Int8(10);
        var c = Int8(20);

        expect(a >= b, isTrue);
        expect(a >= c, isTrue);
        expect(b >= a, isFalse);
      });
    });

    group('Equality and Hash Code', () {
      test('Equality works with FixedNum and raw numbers', () {
        var a = Int8(42);
        var b = Int8(42);
        var c = Int8(43);
        var d = FixedNum.from(42, type: 'int16');

        expect(a == b, isTrue);
        expect(a == c, isFalse);
        expect(a == 42, isTrue);
        expect(a == 43, isFalse);
        expect(a == d, isTrue); // Different types but same value
      });

      test('Hash code is consistent with equals', () {
        var a = Int8(42);
        var b = Int8(42);
        var c = Int8(43);

        expect(a.hashCode == b.hashCode, isTrue);
        expect(a.hashCode == c.hashCode, isFalse);
      });
    });

    group('Common Methods', () {
      test('abs() works correctly', () {
        var a = Int8(42);
        var b = Int8(-42);

        expect(a.abs(), 42);
        expect(b.abs(), 42);
      });

      test('sign works correctly', () {
        var a = Int8(42);
        var b = Int8(-42);
        var c = Int8(0);

        expect(a.sign, 1);
        expect(b.sign, -1);
        expect(c.sign, 0);
      });

      test('isNegative works correctly', () {
        var a = Int8(42);
        var b = Int8(-42);
        var c = Int8(0);

        expect(a.isNegative, isFalse);
        expect(b.isNegative, isTrue);
        expect(c.isNegative, isFalse);
      });
    });

    group('Type Conversions', () {
      test('toInt() works correctly', () {
        var a = Int8(42);
        var b = Int8(-42);

        expect(a.toInt(), 42);
        expect(b.toInt(), -42);
      });

      test('toDouble() works correctly', () {
        var a = Int8(42);
        var b = Int8(-42);

        expect(a.toDouble(), 42.0);
        expect(b.toDouble(), -42.0);
      });

      test('Conversion to other fixed types works', () {
        var a = Int8(42);

        expect(a.toInt16(), isA<Int16>());
        expect(a.toInt16().value, 42);

        expect(a.toInt32(), isA<Int32>());
        expect(a.toInt32().value, 42);

        expect(a.toFloat32(), isA<Float32>());
        expect(a.toFloat32().value, 42.0);
      });

      test('toString() works correctly', () {
        var a = Int8(42);
        var b = Int8(-42);

        expect(a.toString(), '42');
        expect(b.toString(), '-42');
      });
    });

    group('Edge Cases and Random Tests', () {
      test('Test with random values for overflow behavior', () {
        // Instead of property-based testing, we'll use a set of random values
        final random = [
          27, 99, 128, 129, 255, 256, 300, -1, -127, -128, -129, -200, -256
        ];

        for (final value in random) {
          final int8 = Int8(value);
          expect(int8.value >= Int8.MIN_VALUE && int8.value <= Int8.MAX_VALUE, isTrue,
              reason: 'Value $value should be converted to a valid Int8 range');
        }
      });

      test('Test arithmetic operations with random values', () {
        final values = [10, 50, 100, 127, -10, -50, -100, -128];

        for (final a in values) {
          for (final b in values) {
            if (b != 0) { // Avoid division by zero
              final sum = Int8(a) + Int8(b);
              final diff = Int8(a) - Int8(b);
              final prod = Int8(a) * Int8(b);

              expect(sum.value >= Int8.MIN_VALUE && sum.value <= Int8.MAX_VALUE, isTrue);
              expect(diff.value >= Int8.MIN_VALUE && diff.value <= Int8.MAX_VALUE, isTrue);
              expect(prod.value >= Int8.MIN_VALUE && prod.value <= Int8.MAX_VALUE, isTrue);
            }
          }
        }
      });
    });
  });
}