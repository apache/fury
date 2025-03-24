import 'dart:collection';

import 'package:fury/src/const/ref_flag.dart';
import 'package:fury/src/resolver/ref/ser_ref_resolver.dart';

final class SerMapRefResolver extends SerRefResolver {
  static final SerRefRes noRef = (refFlag: RefFlag.NULL, refId: null);

  final Map<int, int> idenHashToRefId = HashMap();

  @override
  SerRefRes getRefId(Object? obj) {
    if (obj == null) {
      return noRef;
    }
    int idenHash = identityHashCode(obj);
    int? refId = idenHashToRefId[idenHash];
    if (refId != null) {
      return (refFlag: RefFlag.TRACK_ALREADY, refId: refId);
    } else {
      // first time
      refId = idenHashToRefId.length;
      idenHashToRefId[idenHash] = refId;
      return (refFlag: RefFlag.TRACK_FIRST, refId: null);
    }
  }
}