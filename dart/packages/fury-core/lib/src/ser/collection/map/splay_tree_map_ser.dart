import 'dart:collection';

import 'package:fury_core/src/ser/collection/collection_ser_cache.dart';

import '../../ser.dart' show Ser;
import '../../ser_cache.dart';
import 'map_ser.dart';

final class _SplayTreeMapSerCache extends CollectionSerCache{
  static SplayTreeMapSer? _serRef;
  static SplayTreeMapSer? _serNoRef;

  const _SplayTreeMapSerCache();

  @override
  Ser getSerWithRef(bool writeRef){
    if (writeRef){
      _serRef ??= SplayTreeMapSer._(true);
      return _serRef!;
    } else {
      _serNoRef ??= SplayTreeMapSer._(false);
      return _serNoRef!;
    }
  }
}


final class SplayTreeMapSer extends MapSer<SplayTreeMap<Object?,Object?>> {

  static const SerCache cache = _SplayTreeMapSerCache();

  SplayTreeMapSer._(super.writeRef);

  @override
  SplayTreeMap<Object?, Object?> newMap(int size) {
    return SplayTreeMap<Object?, Object?>();
  }
}