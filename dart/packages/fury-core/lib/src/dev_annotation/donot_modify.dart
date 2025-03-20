import 'package:meta/meta_meta.dart';

@Target({TargetKind.function, TargetKind.method})
class DoNotModifyReturn {
  const DoNotModifyReturn();
}

const doNotModifyReturn = DoNotModifyReturn();