import 'package:collection/collection.dart';
import 'package:fury_core/src/meta/specs/custom_type_spec.dart';
import 'package:meta/meta.dart';

import '../../const/obj_type.dart';

// enum 不需要tag，因为不允许直接传输一个enum,它应该总在一个类中
// enum 不支持继承，这就为序列化提供了很多方便，不会出现不知道具体类的情况了
@immutable
class EnumSpec extends CustomTypeSpec{
  // final String tag;
  // TODO: 现在的enum仅支持使用ordinal作为传输，还有有支持FuryEnum注解,例如使用value之类的，所以这里直接使用values数组就可以了
  final List<Enum> values;
  const EnumSpec(Type dartType, /*this.tag,*/ this.values): super(dartType, ObjType.NAMED_ENUM);

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