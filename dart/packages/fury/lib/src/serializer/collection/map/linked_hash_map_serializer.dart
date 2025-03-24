import 'dart:collection';
import 'package:fury/src/serializer/collection/collection_serializer_cache.dart';
import 'package:fury/src/serializer/collection/map/map_serializer.dart';
import 'package:fury/src/serializer/serializer.dart';
import 'package:fury/src/serializer/serializer_cache.dart';

final class _LinkedHashMapSerializerCache extends CollectionSerializerCache{
  static LinkedHashMapSerializer? _serRef;
  static LinkedHashMapSerializer? _serNoRef;

  const _LinkedHashMapSerializerCache();

  @override
  Serializer getSerWithRef(bool writeRef){
    if (writeRef){
      _serRef ??= LinkedHashMapSerializer._(true);
      return _serRef!;
    } else {
      _serNoRef ??= LinkedHashMapSerializer._(false);
      return _serNoRef!;
    }
  }
}

final class LinkedHashMapSerializer extends MapSerializer<LinkedHashMap<Object?,Object?>> {

  static const SerializerCache cache = _LinkedHashMapSerializerCache();

  LinkedHashMapSerializer._(super.writeRef);

  @override
  LinkedHashMap<Object?, Object?> newMap(int size) {
    return LinkedHashMap<Object?, Object?>();
  }
}