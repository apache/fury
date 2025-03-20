import 'dart:collection';

import 'package:fury_core/src/ser/collection/collection_ser_cache.dart';

import '../../ser.dart' show Ser;
import '../../ser_cache.dart';
import 'map_ser.dart';

final class _HashMapSerCache extends CollectionSerCache{
  static HashMapSer? _serRef;
  static HashMapSer? _serNoRef;

  const _HashMapSerCache();

  @override
  Ser getSerWithRef(bool writeRef){
    if (writeRef){
      _serRef ??= HashMapSer._(true);
      return _serRef!;
    } else {
      _serNoRef ??= HashMapSer._(false);
      return _serNoRef!;
    }
  }
}


final class HashMapSer extends MapSer<HashMap<Object?,Object?>> {

  static const SerCache cache = _HashMapSerCache();

  HashMapSer._(super.writeRef);

  @override
  HashMap<Object?, Object?> newMap(int size) {
    return HashMap<Object?, Object?>();
  }
}