import 'dart:collection';

import 'package:fury_core/src/ser/collection/collection_ser_cache.dart';
import 'package:fury_core/src/ser/collection/set/set_ser.dart';

import '../../ser.dart' show Ser;
import '../../ser_cache.dart';

final class _HashSetSerCache extends CollectionSerCache{
  static HashSetSer? _serRef;
  static HashSetSer? _serNoRef;

  const _HashSetSerCache();

  @override
  Ser getSerWithRef(bool writeRef){
    if (writeRef){
      _serRef ??= HashSetSer._(true);
      return _serRef!;
    } else {
      _serNoRef ??= HashSetSer._(false);
      return _serNoRef!;
    }
  }
}


final class HashSetSer extends SetSer{

  static const SerCache cache = _HashSetSerCache();
  static const Object obj = Object();

  HashSetSer._(super.writeRef);

  @override
  Set newSet(bool nullable) {
    return nullable ? HashSet<Object?>() : HashSet<Object>();
  }
}