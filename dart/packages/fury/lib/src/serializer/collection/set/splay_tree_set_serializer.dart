import 'dart:collection';
import 'package:fury/src/serializer/collection/collection_serializer_cache.dart';
import 'package:fury/src/serializer/collection/set/set_serializer.dart';
import 'package:fury/src/serializer/serializer.dart';
import 'package:fury/src/serializer/serializer_cache.dart';

final class _SplayTreeSetSerializerCache extends CollectionSerializerCache{
  static SplayTreeSetSerializer? _serRef;
  static SplayTreeSetSerializer? _serNoRef;

  const _SplayTreeSetSerializerCache();

  @override
  Serializer getSerWithRef(bool writeRef){
    if (writeRef){
      _serRef ??= SplayTreeSetSerializer._(true);
      return _serRef!;
    } else {
      _serNoRef ??= SplayTreeSetSerializer._(false);
      return _serNoRef!;
    }
  }
}

final class SplayTreeSetSerializer extends SetSerializer{

  static const SerializerCache cache = _SplayTreeSetSerializerCache();
  static const Object obj = Object();

  SplayTreeSetSerializer._(super.writeRef);

  @override
  Set newSet(bool nullable) {
    return nullable ? SplayTreeSet<Object?>() : SplayTreeSet<Object>();
  }
}