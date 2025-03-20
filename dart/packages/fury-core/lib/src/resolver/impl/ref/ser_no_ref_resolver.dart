import 'package:fury_core/src/const/ref_flag.dart';
import 'package:fury_core/src/dev_annotation/optimize.dart';
import 'package:fury_core/src/resolver/ref/ser_ref_resolver.dart';
final class SerNoRefResolver extends SerRefResolver{

  // singleton
  static const SerNoRefResolver _instance = SerNoRefResolver._();
  factory SerNoRefResolver() => _instance;
  // private constructor
  const SerNoRefResolver._();

  static final SerRefRes noRef = (refFlag: RefFlag.NULL, refId: null);
  static final SerRefRes untrackNotNull = (refFlag: RefFlag.UNTRACK_NOT_NULL, refId: null);

  @inline
  @override
  SerRefRes getRefId(Object? obj) {
    return obj == null ? noRef : untrackNotNull;
  }

  @override
  @inline
  RefFlag getRefFlag(Object? obj) {
    return obj == null ? RefFlag.NULL : RefFlag.UNTRACK_NOT_NULL;
  }
}