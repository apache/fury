import 'package:fury_core/fury_core.dart';
part '../generated/simple_struct1.g.dart';

@furyClass
class SimpleStruct1 with _$SimpleStruct1Fury {
  late Int32 a;

  @override
  bool operator ==(Object other) {
    if (other is! SimpleStruct1) return false;
    return a == other.a;
  }
}