import 'package:meta/meta_meta.dart';
import 'package:fury/src/annotation/fury_object.dart';

/// An annotation that provides Fury serialization and deserialization support for classes.
/// 
/// Apply this annotation to classes that need to be serialized/deserialized by Fury.
/// By default, Fury will use available parameterless constructors for object creation
/// during deserialization.
@Target({TargetKind.classType})
class FuryClass extends FuryObject{
  static const String name = 'FuryClass';
  static const List<TargetKind> targets = [TargetKind.classType];

  /// Indicates whether the class promises to be acyclic (contains no circular references).
  ///
  /// When set to true, Fury can optimize serialization by skipping circular reference checks.
  /// Note: Even with this set to true, Fury will still use an available parameterless 
  /// constructor if one exists.
  final bool promiseAcyclic;

  /// Creates a FuryClass annotation.
  /// 
  /// [promiseAcyclic] - Set to true if the class guarantees no circular references.
  const FuryClass({this.promiseAcyclic = false});
}

/// A convenience constant instance of [FuryClass] with default values.
const FuryClass furyClass = FuryClass();
