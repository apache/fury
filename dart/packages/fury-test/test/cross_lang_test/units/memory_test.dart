// @Skip()
library;

import 'dart:io';
import 'dart:typed_data';

import 'package:checks/checks.dart';
import 'package:fury_core/fury_core.dart';
import 'package:fury_test/extensions/array_ext.dart';
import 'package:test/test.dart';

import '../../util/test_file_util.dart';
import '../cross_lang_util.dart';

void main() {
  test('testBuffer', () {
    ByteWriter bw = ByteWriter();
    bw.writeBool(true);
    bw.writeInt8(Int8.maxValue.value);
    bw.writeInt16(Int16.maxValue.value);
    bw.writeInt32(Int32.maxValue.value);
    bw.writeInt64(0x7FFFFFFFFFFFFFFF);
    bw.writeFloat32(Float32(-1.1).value);
    bw.writeFloat64(-1.1);
    bw.writeVarUint32(100);
    Uint8List bytes2 = Uint8List.fromList([97,98]);
    bw.writeInt32(bytes2.lengthInBytes);
    bw.writeBytes(bytes2);

    File file = TestFileUtil.getWriteFile('test_buffer.data', bw.toBytes());
    bool exeRes = CrossLangUtil.executeWithPython('test_buffer', file.path);
    check(exeRes).isTrue();

    Uint8List readBytes = file.readAsBytesSync();
    ByteReader br = ByteReader.forBytes(readBytes);
    check(br.readBool()).isTrue();
    check(br.readInt8()).equals(Int8.maxValue.value);
    check(br.readInt16()).equals(Int16.maxValue.value);
    check(br.readInt32()).equals(Int32.maxValue.value);
    check(br.readInt64()).equals(0x7FFFFFFFFFFFFFFF);
    check(br.readFloat32()).equals(Float32(-1.1).value);
    check(br.readFloat64()).equals(-1.1);
    check(br.readVarUint32()).equals(100);
    Uint8List byteLis = br.copyBytes(br.readInt32());
    check(byteLis.memEquals(bytes2)).isTrue();
  });
}