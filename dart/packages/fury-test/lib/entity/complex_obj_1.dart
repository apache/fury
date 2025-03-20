import 'dart:typed_data';
import 'package:fury_core/fury_core.dart';
import 'package:fury_test/extensions/map_ext.dart';

part '../generated/complex_obj_1.g.dart';
////
@furyClass
class ComplexObject1 with _$ComplexObject1Fury{
  late Object f1;
  late String f2;
  late List<Object> f3;
  late Map<Int8, Int32> f4;
  late Int8 f5;
  late Int16 f6;
  late Int32 f7;
  late int f8;
  late Float32 f9;
  late double f10;
  late Int16List f11;
  late List<Int16> f12;

  // define ==
  @override
  bool operator ==(Object other) {
    return identical(this, other) ||
        (other is ComplexObject1 &&
            runtimeType == other.runtimeType &&
            f1 == other.f1 &&
            f2 == other.f2 &&
            f3.equals(other.f3) &&
            f4.equals(other.f4) &&
            f5 == other.f5 &&
            f6 == other.f6 &&
            f7 == other.f7 &&
            f8 == other.f8 &&
            f9 == other.f9 &&
            f10 == other.f10 &&
            f11.equals(other.f11) &&
            f12.equals(other.f12));
  }
}