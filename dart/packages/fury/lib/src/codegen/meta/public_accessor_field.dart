import 'package:meta/meta.dart';

/// During the Dart analyzer analysis phase, both setters and getters are analyzed as FieldElement.
/// Here, AccessorField is used to represent them.
@immutable
class PublicAccessorField{
  final String name;
  final bool hasSetter;
  final bool hasGetter;

  const PublicAccessorField(this.name, this.hasSetter, this.hasGetter);
}
