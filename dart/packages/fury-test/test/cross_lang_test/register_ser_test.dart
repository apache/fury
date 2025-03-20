// @Skip()
library;

import 'dart:io';
import 'dart:typed_data';

import 'package:checks/checks.dart';
import 'package:fury_core/fury_core.dart';
import 'package:fury_core/src/deser_pack.dart';
import 'package:fury_core/src/ser_pack.dart';
import 'package:fury_test/entity/complex_obj_1.dart';
import 'package:test/test.dart';

import '../util/test_file_util.dart';
import 'cross_lang_util.dart';

final class ComplexObject1Serializer extends Ser<ComplexObject1>{

  const ComplexObject1Serializer(): super(ObjType.NAMED_STRUCT, true);

  @override
  ComplexObject1 read(ByteReader br, int refId, DeserPack pack) {
    ComplexObject1 obj = ComplexObject1();
    pack.refResolver.setRefTheLatestId(obj);
    obj.f1 = pack.furyDeser.xReadRefNoSer(br, pack)!;
    obj.f2 = pack.furyDeser.xReadRefNoSer(br, pack)! as String;
    obj.f3 = (pack.furyDeser.xReadRefNoSer(br, pack)! as List).cast<Object>();
    return obj;
  }

  @override
  void write(ByteWriter bw, ComplexObject1 v, SerPack pack) {
    pack.furySer.xWriteRefNoSer(bw, v.f1, pack);
    pack.furySer.xWriteRefNoSer(bw, v.f2, pack);
    pack.furySer.xWriteRefNoSer(bw, v.f3, pack);
  }
}

void main() {
  group('A group of tests', () {

    test('testRegisterSerializer', () {
      Fury fury = Fury(
        xlangMode: true,
        refTracking: true,
      );
      fury.register($ComplexObject1,"test.ComplexObject1");
      fury.registerSerializer(ComplexObject1, const ComplexObject1Serializer());

      ComplexObject1 obj = ComplexObject1();
      obj.f1 = true;
      obj.f2 = "abc";
      obj.f3 = ['abc','abc'];

      Uint8List bytes = fury.toFury(obj);
      Object? obj2 = fury.fromFury(bytes);
      check(obj2).isA<ComplexObject1>();
      ComplexObject1 obj3 = obj2 as ComplexObject1;
      check(obj3.f1 as bool).isTrue();
      check(obj3.f2).equals("abc");
      check(obj3.f3.equals(['abc','abc'])).isTrue();

      File file = TestFileUtil.getWriteFile("test_register_serializer", bytes);
      bool exeRes = CrossLangUtil.executeWithPython("test_register_serializer", file.path);
      check(exeRes).isTrue();
      Object? deObj = fury.fromFury(file.readAsBytesSync());
      check(deObj).isA<ComplexObject1>();
      ComplexObject1 obj4 = deObj as ComplexObject1;
      check(obj4.f1 as bool).isTrue();
      check(obj4.f2).equals("abc");
      check(obj4.f3.equals(['abc','abc'])).isTrue();
    });

  });
}