import 'package:fury_core/src/meta/specs/enum_spec.dart';
import 'package:fury_core/src/ser/ser.dart' show Ser;

final class EnumSpecWrap{
  final List<Object> values;
  late final Ser ser;

  EnumSpecWrap._(
    this.values,
  );

  factory EnumSpecWrap.of(
    EnumSpec spec,
  ){
    return EnumSpecWrap._(spec.values);
  }
}