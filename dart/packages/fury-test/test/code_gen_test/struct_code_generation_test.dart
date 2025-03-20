// @Skip()
library;


import 'package:checks/checks.dart';
import 'package:fury_core/fury_core.dart';
import 'package:fury_test/entity/complex_obj_2.dart';
import 'package:test/test.dart';

void main(){
  group('Simple Struct Code Generation', () {

    test('test struct spec generation', () async {
      ClassSpec spec = ClassSpec(
        ComplexObject2,
        true, false,
        [
          FieldSpec(
            'f1',
            TypeSpec(Object, ObjType.UNKNOWN_YET, false, false, null, const [],),
            true,
            true,
            (Object inst) => (inst as ComplexObject2).f1,
            null,
          ),
          FieldSpec(
            'f2',
            TypeSpec(Map, ObjType.MAP, false, false, null,
              const [
                TypeSpec(Int8, ObjType.INT8, false, true, null, [],),
                TypeSpec(Int32, ObjType.INT32, false, true, null, [],),
              ],
            ),
            true,
            true,
            (Object inst) => (inst as ComplexObject2).f2,
            null,
          ),
        ],
        (List<dynamic> objList) => ComplexObject2(
          (objList[0] as Object),
          Map<Int8, Int32>.of(
              (objList[1] as Map).map((k, v) => MapEntry((k as Int8), (v as Int32)))),
        ),
        null,
      );
      check($ComplexObject2).equals(spec);
    });
  });
}