import 'dart:collection';
import 'package:fury/src/serializer/collection/collection_serializer_cache.dart';
import 'package:fury/src/serializer/collection/map/map_serializer.dart';
import 'package:fury/src/serializer/serializer.dart';
import 'package:fury/src/serializer/serializer_cache.dart';

final class _HashMapSerializerCache extends CollectionSerializerCache{
  static HashMapSerializer? _serRef;
  static HashMapSerializer? _serNoRef;

  const _HashMapSerializerCache();

  @override
  Serializer getSerWithRef(bool writeRef){
    if (writeRef){
      _serRef ??= HashMapSerializer._(true);
      return _serRef!;
    } else {
      _serNoRef ??= HashMapSerializer._(false);
      return _serNoRef!;
    }
  }
}

final class HashMapSerializer extends MapSerializer<HashMap<Object?,Object?>> {

  static const SerializerCache cache = _HashMapSerializerCache();

  HashMapSerializer._(super.writeRef);

  @override
  HashMap<Object?, Object?> newMap(int size) {
    return HashMap<Object?, Object?>();
  }
}