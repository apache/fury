import 'package:meta/meta_meta.dart';

@Target({TargetKind.parameter})
class MayBeModified {
  const MayBeModified();
}

const mayBeModified = MayBeModified();