import 'package:fury_core/src/annotation/fury_obj.dart';
import 'package:meta/meta_meta.dart';

@Target({TargetKind.classType})
class FuryClass extends FuryObj{
  static const String name = 'FuryClass';
  static const List<TargetKind> targets = [TargetKind.classType];

  // 即使是promiseAcyclic, 如果有可以使用并且符合要求的无参构造函数，也会优先使用
  final bool promiseAcyclic;

  const FuryClass({this.promiseAcyclic = false});
}

const FuryClass furyClass = FuryClass();