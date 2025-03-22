import 'dart:collection';

import '../../../const/ref_flag.dart' show RefFlag;
import '../../ref/ser_ref_resolver.dart';

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
      // 第一次出现
      refId = idenHashToRefId.length;
      idenHashToRefId[idenHash] = refId;
      return (refFlag: RefFlag.TRACK_FIRST, refId: null);
    }
  }
}