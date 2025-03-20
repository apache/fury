import 'package:collection/collection.dart';
import 'package:fury_core/src/const/obj_type.dart';
import 'package:fury_core/src/meta/specs/custom_type_spec.dart';

import 'field_spec.dart';

typedef HasArgsCons = Object Function(List<Object?>);
typedef NoArgsCons = Object Function();

class ClassSpec extends CustomTypeSpec{
  final List<FieldSpec> fields;
  final bool promiseAcyclic;
  final bool noCyclicRisk; // 通过分析代码得到的没有循环引用的风险，例如所有fields均为
  final HasArgsCons? construct;
  // 思考，一定需要noArgs这么严格吗？如果提供了只有基本类型参数的构造函数，可以吗？
  // 不可以，反序列化阶段时字段顺序不是随心所欲的，是按照字段排序来的（基本类型并不一定在排序的的开头）
  final NoArgsCons? noArgConstruct;

  ClassSpec(
    Type dartType,
    this.promiseAcyclic,
    this.noCyclicRisk,
    this.fields,
    this.construct,
    this.noArgConstruct,
  ): super(dartType, ObjType.NAMED_STRUCT) // 目前类就只能是命名结构体
  {
    assert(construct != null || noArgConstruct != null, 'construct and noArgConstruct can not be both non-null');
    // 如果noArgConstruct为null, 则必定要求了promiseAcyclic, A->B 等价于 !A || B
    assert(noArgConstruct != null || promiseAcyclic || noCyclicRisk);
  }

  @override
  bool operator ==(Object other) {
    return identical(this, other) ||
      (other is ClassSpec &&
        runtimeType == other.runtimeType &&
        fields.equals(other.fields) &&
        promiseAcyclic == other.promiseAcyclic &&
        noCyclicRisk == other.noCyclicRisk &&
        ((identical(construct, other.construct)) || (construct == null)==(construct == null)) &&
        ((identical(noArgConstruct, other.noArgConstruct)) || (noArgConstruct == null)==(other.noArgConstruct == null))
      );
  }

}