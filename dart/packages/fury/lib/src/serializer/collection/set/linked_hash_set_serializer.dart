import 'dart:collection';
import 'package:fury/src/serializer/collection/collection_serializer_cache.dart';
import 'package:fury/src/serializer/collection/set/set_serializer.dart';
import 'package:fury/src/serializer/serializer.dart';
import 'package:fury/src/serializer/serializer_cache.dart';

final class _LinkedHashSetSerializerCache extends CollectionSerializerCache{
  static LinkedHashSetSerializer? _serRef;
  static LinkedHashSetSerializer? _serNoRef;

  const _LinkedHashSetSerializerCache();

  @override
  Serializer getSerWithRef(bool writeRef){
    if (writeRef){
      _serRef ??= LinkedHashSetSerializer._(true);
      return _serRef!;
    } else {
      _serNoRef ??= LinkedHashSetSerializer._(false);
      return _serNoRef!;
    }
  }
}

final class LinkedHashSetSerializer extends SetSerializer{

  static const SerializerCache cache = _LinkedHashSetSerializerCache();
  static const Object obj = Object();

  LinkedHashSetSerializer._(super.writeRef);

  @override
  Set newSet(bool nullable) {
    return nullable ? LinkedHashSet<Object?>() : LinkedHashSet<Object>();
  }
}
