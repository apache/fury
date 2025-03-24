// @Skip()
library;

import 'package:checks/checks.dart';
import 'package:fury/fury.dart';
import 'package:fury_test/entity/enum_foo.dart';
import 'package:test/test.dart';

void main(){
  group('Simple Enum Code Generation', () {

    test('test enum spec generation', () async {
      EnumSpec enumSpec = EnumSpec(
        EnumFoo,
        [EnumFoo.A, EnumFoo.B]
      );
      EnumSpec enumSubClassSpec = EnumSpec(
        EnumSubClass,
        [EnumSubClass.A, EnumSubClass.B]
      );
      check($EnumFoo).equals(enumSpec);
      check($EnumSubClass).equals(enumSubClassSpec);
    });
  });
}