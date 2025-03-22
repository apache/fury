import 'dart:collection';

import 'package:fury_core/src/ser/collection/collection_ser_cache.dart';
import 'package:fury_core/src/ser/collection/set/set_ser.dart';

import '../../ser.dart' show Ser;
import '../../ser_cache.dart';

final class _SplayTreeSetSerCache extends CollectionSerCache{
  static SplayTreeSetSer? _serRef;
  static SplayTreeSetSer? _serNoRef;

  const _SplayTreeSetSerCache();

  @override
  Ser getSerWithRef(bool writeRef){
    if (writeRef){
      _serRef ??= SplayTreeSetSer._(true);
      return _serRef!;
    } else {
      _serNoRef ??= SplayTreeSetSer._(false);
      return _serNoRef!;
    }
  }
}


final class SplayTreeSetSer extends SetSer{

  static const SerCache cache = _SplayTreeSetSerCache();
  static const Object obj = Object();

  SplayTreeSetSer._(super.writeRef);

  @override
  Set newSet(bool nullable) {
    return nullable ? SplayTreeSet<Object?>() : SplayTreeSet<Object>();
  }
}