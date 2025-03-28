import 'dart:collection';
import 'package:fury/src/serializer/collection/collection_serializer_cache.dart';
import 'package:fury/src/serializer/collection/map/map_serializer.dart';
import 'package:fury/src/serializer/serializer.dart';
import 'package:fury/src/serializer/serializer_cache.dart';

final class _SplayTreeMapSerializerCache extends CollectionSerializerCache{
  static SplayTreeMapSerializer? _serRef;
  static SplayTreeMapSerializer? _serNoRef;

  const _SplayTreeMapSerializerCache();

  @override
  Serializer getSerWithRef(bool writeRef){
    if (writeRef){
      _serRef ??= SplayTreeMapSerializer._(true);
      return _serRef!;
    } else {
      _serNoRef ??= SplayTreeMapSerializer._(false);
      return _serNoRef!;
    }
  }
}

final class SplayTreeMapSerializer extends MapSerializer<SplayTreeMap<Object?,Object?>> {

  static const SerializerCache cache = _SplayTreeMapSerializerCache();

  SplayTreeMapSerializer._(super.writeRef);

  @override
  SplayTreeMap<Object?, Object?> newMap(int size) {
    return SplayTreeMap<Object?, Object?>();
  }
}