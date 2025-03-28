import 'package:meta/meta_meta.dart';

/// Annotation used to specify which constructor should be used 
/// when Fury deserializes an object.
///
/// Apply this annotation to a constructor to indicate it should be used
/// during the deserialization process by Fury.
///
/// Example:
/// ```dart
/// class Person {
///   late final String name;
///   late final int age;
///
///   // Alternative constructor that won't be used for deserialization
///   Person(this.name, this.age);
///
///   // This constructor will be used by Fury during deserialization
///   @furyConstructor
///   Person.guest();
/// }
/// ```
@Target({TargetKind.constructor})
class FuryConstructor {
  /// The name identifier for this annotation.
  static const String name = 'FuryCons';
  
  /// The valid targets where this annotation can be applied.
  static const List<TargetKind> targets = [TargetKind.constructor];

  /// Creates a new [FuryConstructor] annotation.
  const FuryConstructor();
}

/// A constant instance of [FuryConstructor] for convenient use.
const FuryConstructor furyConstructor = FuryConstructor();
