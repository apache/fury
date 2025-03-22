// @Skip()
library;

import 'dart:io';
import 'dart:typed_data';

import 'package:checks/checks.dart';
import 'package:fury_core/fury_core.dart';
import 'package:fury_core/fury_core_test.dart';
import 'package:fury_test/entity/complex_obj_1.dart';
import 'package:test/test.dart';

import '../../util/test_file_util.dart';
import '../cross_lang_util.dart';

void main() {
  group('serialization algorithm test', () {

    test('testMurmurHash3', () {
      Uint8List bytes = Uint8List(16);
      bytes[0] = 0x01;
      bytes[1] = 0x02;
      bytes[2] = 0x08;
      var hashPair = Murmur3Hash.hash128x64(bytes, 3);
      ByteData byteData = ByteData.view(bytes.buffer);
      byteData.setUint64(0, hashPair.$1, Endian.little);
      byteData.setUint64(8, hashPair.$2, Endian.little);
      File file = TestFileUtil.getWriteFile('test_murmurhash3.data', bytes);
      bool exeRes = CrossLangUtil.executeWithPython('test_murmurhash3', file.path);
      check(exeRes).isTrue();
    });

    test('testStructHash', () {
      Fury fury = Fury(
        xlangMode: true,
        refTracking: true,
      );
      fury.register($ComplexObject1, "test.ComplexObject1");
      var hashPair = fury.getStructHashPair(ComplexObject1);
      ByteData byteData = ByteData(4);
      byteData.setUint32(0, hashPair.fromFuryHash, Endian.little);
      File file = TestFileUtil.getWriteFile('test_struct_hash', byteData.buffer.asUint8List());
      bool exeRes = CrossLangUtil.executeWithPython('test_struct_hash', file.path);
      check(exeRes).isTrue();
    });
  });
}