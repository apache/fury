import 'package:fury/fury.dart';
import 'package:fury_test/extensions/map_ext.dart';

part '../generated/complex_obj_2.g.dart';

@FuryClass(promiseAcyclic: true)
class ComplexObject2 with _$ComplexObject2Fury {
  final Object f1;
  final Map<Int8, Int32> f2;

  const ComplexObject2(this.f1, this.f2);

  @override
  bool operator ==(Object other) {
    return identical(this, other) ||
      (other is ComplexObject2 &&
          runtimeType == other.runtimeType &&
          f1 == other.f1 &&
          f2.equals(other.f2)
      );
  }
}