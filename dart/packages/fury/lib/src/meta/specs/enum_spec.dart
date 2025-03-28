import 'package:collection/collection.dart';
import 'package:meta/meta.dart';
import 'package:fury/src/meta/specs/custom_type_spec.dart';
import 'package:fury/src/const/obj_type.dart';

// Enums do not need tags because they are not allowed to be transmitted directly; they should always be within a class.
// Enums do not support inheritance, which makes serialization much easier as there will be no cases where the specific class is unknown.
@immutable
class EnumSpec extends CustomTypeSpec{
  // final String tag;
  // TODO: Currently, enums only support using ordinal for transmission. There is also support for FuryEnum annotation, such as using value, so we can directly use the values array here.
  final List<Enum> values;
  const EnumSpec(Type dartType, this.values): super(dartType, ObjType.NAMED_ENUM);

  @override
  bool operator ==(Object other) {
    return
      identical(this, other) ||
      other is EnumSpec &&
        runtimeType == other.runtimeType &&
        dartType == other.dartType &&
        values.equals(other.values);
  }
}
