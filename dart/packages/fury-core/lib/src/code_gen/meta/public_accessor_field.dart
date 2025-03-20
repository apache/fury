import 'package:meta/meta.dart';

/// dart的analyser分析阶段，会把setter 和getter都分析成FieldElement, 这里使用 AccessorField来表示
@immutable
class PublicAccessorField{
  final String name;
  final bool hasSetter;
  final bool hasGetter;

  const PublicAccessorField(this.name, this.hasSetter, this.hasGetter);
}