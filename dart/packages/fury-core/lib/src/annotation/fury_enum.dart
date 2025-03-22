import 'package:meta/meta_meta.dart';

import 'fury_obj.dart';

class FuryEnum extends FuryObj {
  static const String name = 'FuryEnum';
  static const List<TargetKind> targets = [TargetKind.enumType];
  // static const List<AnnotationTarget> targets = [AnnotationTarget.CLASS];

  // final String? tag; // if null, use className as type tag.

  const FuryEnum(/*[this.tag]*/);
}

const FuryEnum furyEnum = FuryEnum();