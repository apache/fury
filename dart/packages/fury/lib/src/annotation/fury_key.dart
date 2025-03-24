import 'package:meta/meta_meta.dart';

/// This class is used as a target for fields and includes options to
/// specify whether to include from and to Fury.
///
/// Example:
/// ```
/// @furyClass
/// class MyClass {
///   @furyKey(includeFromFury: false)
///   final String name;
/// }
/// ```
///
/// The `FuryKey` class has two optional parameters:
/// - `includeFromFury`: A boolean value indicating whether to include this field during deserialization.
///   Defaults to `true`.
/// - `includeToFury`: A boolean value indicating whether to include this field during serialization.
///   Defaults to `true`.
@Target({TargetKind.field})
class FuryKey {
  static const String name = 'FuryKey';
  static const List<TargetKind> targets = [TargetKind.field];

  /// A boolean value indicating whether to include this field during deserialization.
  final bool includeFromFury;

  /// A boolean value indicating whether to include this field during serialization.
  final bool includeToFury;

  /// Both [includeFromFury] and [includeToFury] default to `true`.
  const FuryKey({
    this.includeFromFury = true,
    this.includeToFury = true,
  });
}