import 'dart:typed_data';

import 'package:fury/fury.dart';

part 'typed_data_array_example.g.dart';

@furyClass
class TypedDataArrayExample with _$TypedDataArrayExampleFury{
  late final Uint8List bytes;
  late final Int32List nums;
  late final BoolList bools;
}