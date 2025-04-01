import 'package:fury/src/const/ref_flag.dart';
import 'package:fury/src/dev_annotation/optimize.dart';
import 'package:fury/src/resolver/serialization_ref_resolver.dart';

final class SerNoRefResolver extends SerializationRefResolver{
  static const SerNoRefResolver _instance = SerNoRefResolver._();
  factory SerNoRefResolver() => _instance;
  // private constructor
  const SerNoRefResolver._();

  static final SerializationRefMeta noRef = (refFlag: RefFlag.NULL, refId: null);
  static final SerializationRefMeta untrackNotNull = (refFlag: RefFlag.UNTRACK_NOT_NULL, refId: null);

  @inline
  @override
  SerializationRefMeta getRefId(Object? obj) {
    return obj == null ? noRef : untrackNotNull;
  }

  @override
  @inline
  RefFlag getRefFlag(Object? obj) {
    return obj == null ? RefFlag.NULL : RefFlag.UNTRACK_NOT_NULL;
  }
}