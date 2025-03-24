import 'dart:collection';
import 'package:fury/src/serializer/collection/collection_serializer_cache.dart';
import 'package:fury/src/serializer/collection/set/set_serializer.dart';
import 'package:fury/src/serializer/serializer.dart';
import 'package:fury/src/serializer/serializer_cache.dart';

final class _HashSetSerializerCache extends CollectionSerializerCache{
  static HashSetSerializer? _serRef;
  static HashSetSerializer? _serNoRef;

  const _HashSetSerializerCache();

  @override
  Serializer getSerWithRef(bool writeRef){
    if (writeRef){
      _serRef ??= HashSetSerializer._(true);
      return _serRef!;
    } else {
      _serNoRef ??= HashSetSerializer._(false);
      return _serNoRef!;
    }
  }
}


final class HashSetSerializer extends SetSerializer{

  static const SerializerCache cache = _HashSetSerializerCache();
  static const Object obj = Object();

  HashSetSerializer._(super.writeRef);

  @override
  Set newSet(bool nullable) {
    return nullable ? HashSet<Object?>() : HashSet<Object>();
  }
}