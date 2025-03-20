import 'package:meta/meta_meta.dart';

@Target({TargetKind.constructor})
class FuryCons {
  static const String name = 'FuryCons';
  static const List<TargetKind> targets = [TargetKind.constructor];

  const FuryCons();
}

const FuryCons furyCons = FuryCons();