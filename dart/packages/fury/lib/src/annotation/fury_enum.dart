import 'package:meta/meta_meta.dart';
import 'fury_object.dart';

/// A class representing an enumeration type in the Fury framework.
/// 
/// This class extends [FuryObject] and is used to annotate enum types
/// within the Fury framework.
/// Example:
/// ```
/// @furyEnum
/// enum Color {
///   // enums
/// }
/// ```
class FuryEnum extends FuryObject {
  static const String name = 'FuryEnum';
  static const List<TargetKind> targets = [TargetKind.enumType];
  
  /// Creates a new instance of [FuryEnum].
  const FuryEnum();
}

/// A constant instance of [FuryEnum].
const FuryEnum furyEnum = FuryEnum();
