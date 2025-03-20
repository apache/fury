import 'package:meta/meta_meta.dart';

@Target({TargetKind.field})
class FuryKey {
  static const String name = 'FuryKey';
  static const List<TargetKind> targets = [TargetKind.field];
  final bool includeFromFury;
  final bool includeToFury;
  // final Object? defaultValue;

  const FuryKey({
    this.includeFromFury = true,
    this.includeToFury = true,
    // this.defaultValue,
  });

  // @override
  // List<TargetKind> get targets => [TargetKind.field];
}