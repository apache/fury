import 'package:fury/src/meta/specs/enum_spec.dart';
import 'package:fury/src/serializer/serializer.dart';

final class EnumSpecWrap{
  final List<Object> values;
  late final Serializer ser;

  EnumSpecWrap._(
    this.values,
  );

  factory EnumSpecWrap.of(
    EnumSpec spec,
  ){
    return EnumSpecWrap._(spec.values);
  }
}