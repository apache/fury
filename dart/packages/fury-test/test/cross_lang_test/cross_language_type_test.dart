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

import 'dart:collection';
import 'dart:io';
import 'dart:typed_data';
import 'package:fury_test/util/cross_lang_util.dart';
import 'package:fury_test/util/test_file_util.dart';
import 'package:test/test.dart';
import 'package:checks/checks.dart';
import 'package:fury/fury.dart';
import 'package:fury_test/entity/enum_foo.dart';
import 'package:fury_test/extensions/array_ext.dart';
import 'package:fury_test/extensions/collection_ext.dart';
import 'package:fury_test/extensions/map_ext.dart';
import 'package:fury_test/extensions/obj_ext.dart';

T Function<T>(Fury, Fury, T) serDe = CrossLangUtil.serDe;

void _testTypedDataArray(Fury fury1, Fury fury2){
  BoolList blist = BoolList.generate(10, (i) => i % 2 == 0);
  check(blist.equals(serDe(fury1, fury2, blist))).isTrue();

  Int8List i8list = Int8List.fromList(List<int>.generate(100, (i) => i * 100));
  check(i8list.memEquals(serDe(fury1, fury2, i8list))).isTrue();

  Int16List i16list = Int16List.fromList(List<int>.generate(100, (i) => i * 100));
  check(i16list.memEquals(serDe(fury1, fury2, i16list))).isTrue();

  Int32List i32list = Int32List.fromList(List<int>.generate(100, (i) => i * -1000));
  check(i32list.memEquals(serDe(fury1, fury2, i32list))).isTrue();

  Int64List i64list = Int64List.fromList(List<int>.generate(100, (i) => i * -10000));
  check(i64list.memEquals(serDe(fury1, fury2, i64list))).isTrue();

  Float32List f32list = Float32List.fromList(List<double>.generate(100, (i) => i * -31415.0));
  check(f32list.memEquals(serDe(fury1, fury2, f32list))).isTrue();

  Float64List f64list = Float64List.fromList(List<double>.generate(100, (i) => i * -314150.0));
  check(f64list.memEquals(serDe(fury1, fury2, f64list))).isTrue();
}

void _testCollectionType(Fury fury1, Fury fury2) {
  List<int> list = List<int>.generate(10, (i) => i * 100);
  List list1 = serDe(fury1, fury2, list);
  check(list1.equals(list)).isTrue();

  List<String> strList = List<String>.generate(10, (i) => 'str$i');
  List strList1 = serDe(fury1, fury2, strList);
  check(strList1.equals(strList)).isTrue();

  SplayTreeSet<Float32> set = SplayTreeSet<Float32>();
  for (int i = 0; i < 10; ++i) {
    set.add(Float32(i * -10.137));
  }
  Object? obj = serDe(fury1, fury2, set);
  check(obj).isA<Set>();
  check((obj as Set).equals(set)).isTrue();
}

void _testArrayCollection(bool refTracking) {
  Fury fury1 = Fury(
    refTracking: refTracking,
  );
  Fury fury2 = Fury(
    refTracking: refTracking,
  );
  _testTypedDataArray(fury1, fury2);
  _testCollectionType(fury1, fury2);
}

void _basicTypeTest(bool refTracking) {
  Fury fury1 = Fury(
    refTracking: refTracking,
  );
  Fury fury2 = Fury(
    refTracking: refTracking,
  );
  check('str').equals(serDe(fury1, fury2, 'str'));
  // with non-latin char
  check('2023年10月23日').equals(serDe(fury1, fury2, '2023年10月23日'));
  check(true).equals(CrossLangUtil.serDe(fury1, fury2, true));

  fury1.register($EnumFoo);
  fury2.register($EnumFoo);
  fury1.register($EnumSubClass);
  fury2.register($EnumSubClass);

  check(EnumFoo.A).equals(serDe(fury1, fury2, EnumFoo.A));
  check(EnumFoo.B).equals(serDe(fury1, fury2, EnumFoo.B));

  check(EnumSubClass.A).equals(serDe(fury1, fury2, EnumSubClass.A));
  check(EnumSubClass.B).equals(serDe(fury1, fury2, EnumSubClass.B));

  LocalDate day = LocalDate.now();
  check(day).equals(serDe(fury1, fury2, day));

  TimeStamp ts = TimeStamp.now();
  check(ts).equals(serDe(fury1, fury2, ts));
}

void main() {
  group('test cross language datatype serialization', () {

    test('testCrossLanguageSerializer', () {
      Fury fury = Fury(
        refTracking: true,
      );
      ByteWriter bw = ByteWriter();
      fury.toFuryWithWriter(true,bw);
      fury.toFuryWithWriter(false, bw);
      fury.toFuryWithWriter(Int32(-1),bw);
      fury.toFuryWithWriter(Int8.maxValue, bw);
      fury.toFuryWithWriter(Int8.minValue, bw);
      fury.toFuryWithWriter(Int16.maxValue, bw);
      fury.toFuryWithWriter(Int16.minValue, bw);
      fury.toFuryWithWriter(Int32.maxValue, bw);
      fury.toFuryWithWriter(Int32.minValue, bw);
      fury.toFuryWithWriter(0x7fffffffffffffff, bw);
      fury.toFuryWithWriter(0x8000000000000000, bw);
      fury.toFuryWithWriter(Float32(-1.0), bw);
      fury.toFuryWithWriter(-1.0, bw);
      fury.toFuryWithWriter('str', bw);

      LocalDate day = LocalDate(2021, 11, 23);
      fury.toFuryWithWriter(day, bw);

      TimeStamp ts = TimeStamp.fromSecondsSinceEpoch(100);
      fury.toFuryWithWriter(ts, bw);

      List<Object> list = ['a', Int32(1), -1.0, ts,day];
      fury.toFuryWithWriter(list, bw);

      Map<Object, Object> map = HashMap();
      for (int i = 0; i < list.length; ++i) {
        map['k$i'] = list[i];
        map[list[i]] = list[i];
      }
      fury.toFuryWithWriter(map, bw);

      Set<Object> set = HashSet.of(list);
      fury.toFuryWithWriter(set, bw);

      BoolList blist = BoolList.of([true, false]);
      fury.toFuryWithWriter(blist, bw);

      Int16List i16list = Int16List.fromList([1,32767]);
      fury.toFuryWithWriter(i16list, bw);

      Int32List i32list = Int32List.fromList([1, 0x7fffffff]);
      fury.toFuryWithWriter(i32list, bw);

      Int64List i64list = Int64List.fromList([1, 0x7fffffffffffffff]);
      fury.toFuryWithWriter(i64list, bw);

      Float32List f32list = Float32List.fromList([1.0, 2.0]);
      fury.toFuryWithWriter(f32list, bw);

      Float64List f64list = Float64List.fromList([1.0, 2.0]);
      fury.toFuryWithWriter(f64list, bw);

      Uint8List bytes1 = bw.takeBytes();

      testFunc(Uint8List bytes) {
        ByteReader br = ByteReader.forBytes(bytes);
        check(fury.fromFury(bytes, br) as bool).isTrue();
        check(fury.fromFury(bytes, br) as bool).isFalse();
        check(Int32(-1) == fury.fromFury(bytes, br)).isTrue();
        check(Int8.maxValue == fury.fromFury(bytes, br)).isTrue();
        check(Int8.minValue == fury.fromFury(bytes, br)).isTrue();
        check(Int16.maxValue == fury.fromFury(bytes, br)).isTrue();
        check(Int16.minValue == fury.fromFury(bytes, br)).isTrue();
        check(Int32.maxValue == fury.fromFury(bytes, br)).isTrue();
        check(Int32.minValue == fury.fromFury(bytes, br)).isTrue();
        check(fury.fromFury(bytes, br) as int).equals(0x7fffffffffffffff);
        check(fury.fromFury(bytes, br) as int).equals(0x8000000000000000);
        check(Float32(-1.0) == fury.fromFury(bytes, br)).isTrue();
        check(fury.fromFury(bytes, br) as double).equals(-1.0);
        check(fury.fromFury(bytes, br) as String).equals('str');

        check(fury.fromFury(bytes, br) as LocalDate).equals(day);
        check(fury.fromFury(bytes, br) as TimeStamp).equals(ts);
        check((fury.fromFury(bytes, br) as List).strEquals(list)).isTrue();
        check((fury.fromFury(bytes, br) as Map).equals(map)).isTrue();
        check((fury.fromFury(bytes, br) as Set).equals(set)).isTrue();
        check((fury.fromFury(bytes, br) as BoolList).equals(blist)).isTrue();
        check((fury.fromFury(bytes, br) as Int16List).equals(i16list)).isTrue();
        check((fury.fromFury(bytes, br) as Int32List).equals(i32list)).isTrue();
        check((fury.fromFury(bytes, br) as Int64List).equals(i64list)).isTrue();
        check((fury.fromFury(bytes, br) as Float32List).equals(f32list)).isTrue();
        check((fury.fromFury(bytes, br) as Float64List).equals(f64list)).isTrue();
      }
      testFunc(bytes1);

      File file = TestFileUtil.getWriteFile('test_cross_language_serializer', bytes1);
      bool exeRes = CrossLangUtil.executeWithPython('test_cross_language_serializer', file.path);
      check(exeRes).isTrue();
      Uint8List bytes2 = file.readAsBytesSync();
      testFunc(bytes2);
    });

    test('Test Basic Types Serialization', () {
      _basicTypeTest(true);
      _basicTypeTest(false);
    });

    test('Test Array and Collection Types Serialization', () {
      _testArrayCollection(true);
      _testArrayCollection(false);
    });
  });
}