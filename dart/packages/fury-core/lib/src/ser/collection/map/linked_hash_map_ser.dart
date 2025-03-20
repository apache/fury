import 'dart:collection';

import 'package:fury_core/src/ser/collection/collection_ser_cache.dart';

import '../../ser.dart' show Ser;
import '../../ser_cache.dart';
import 'map_ser.dart';

final class _LinkedHashMapSerCache extends CollectionSerCache{
  static LinkedHashMapSer? _serRef;
  static LinkedHashMapSer? _serNoRef;

  const _LinkedHashMapSerCache();

  @override
  Ser getSerWithRef(bool writeRef){
    if (writeRef){
      _serRef ??= LinkedHashMapSer._(true);
      return _serRef!;
    } else {
      _serNoRef ??= LinkedHashMapSer._(false);
      return _serNoRef!;
    }
  }
}


final class LinkedHashMapSer extends MapSer<LinkedHashMap<Object?,Object?>> {

  static const SerCache cache = _LinkedHashMapSerCache();

  LinkedHashMapSer._(super.writeRef);

  @override
  LinkedHashMap<Object?, Object?> newMap(int size) {
    return LinkedHashMap<Object?, Object?>();
  }
}