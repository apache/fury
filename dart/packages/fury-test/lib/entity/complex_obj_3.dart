import 'dart:collection';
import 'package:fury_core/fury_core.dart';
import 'package:fury_test/extensions/map_ext.dart';

part '../generated/complex_obj_3.g.dart';


@furyClass
class ComplexObject3 with _$ComplexObject3Fury{
  late final List<Map<int, Float32>> f1;
  late final HashMap<String, List<SplayTreeMap<int, Float32>>> f2;
  late final LinkedHashMap<String, HashSet<Int8>> f3;

  @override
  bool operator ==(Object other) {
    if (identical(this, other)) return true;
    if (other is! ComplexObject3) return false;
    if (other.runtimeType != runtimeType) return false;

    for (int i = 0; i < f1.length; i++) {
      if (!f1[i].equals(other.f1[i])) return false;
    }

    if (f2.length != other.f2.length) return false;
    for (var key in f2.keys) {
      var value1 = other.f2[key];
      if (value1 == null) return false;
      var value = f2[key];
      if (value!.length != value1.length) return false;
      for (int i = 0; i < value.length; i++) {
        if (!value[i].equals(value1[i])) return false;
      }
    }

    if (f3.length != other.f3.length) return false;
    for (var key in f3.keys) {
      var value1 = other.f3[key];
      if (value1 == null) return false;
      var value = f3[key];
      if (value!.length != value1.length) return false;
      for (int i = 0; i < value.length; i++) {
        if (value.elementAt(i) != value1.elementAt(i)) return false;
      }
    }
    return true;
  }
}