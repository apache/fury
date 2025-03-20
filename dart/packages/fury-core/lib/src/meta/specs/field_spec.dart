import 'package:fury_core/src/meta/specs/type_spec.dart';
import 'package:meta/meta.dart';

typedef Getter = Object? Function(Object inst);
typedef Setter = void Function(Object inst, dynamic value);

@immutable
class FieldSpec{
  final String name;
  final TypeSpec typeSpec;
  final Getter? getter;
  final Setter? setter;

  final bool includeFromFury;
  final bool includeToFury;
  
  const FieldSpec(
    this.name,
    this.typeSpec,
    this.includeFromFury,
    this.includeToFury,
    this.getter,
    this.setter,
  );

  /// 关于Function的==比较，除了静态方法可以直接使用==，很难比较函数的功能是否一样，所以为了测试，使用比较null简略比较一下
  @override
  bool operator ==(Object other) {
    return identical(this, other) ||
      (other is FieldSpec &&
        runtimeType == other.runtimeType &&
        name == other.name &&
        typeSpec == other.typeSpec &&
        includeFromFury == other.includeFromFury &&
        includeToFury == other.includeToFury &&
        (identical(getter, other.getter) || (getter == null) == (other.getter == null)) &&
        (identical(setter, other.setter) || (setter == null) == (other.setter == null))
      );
  }
}