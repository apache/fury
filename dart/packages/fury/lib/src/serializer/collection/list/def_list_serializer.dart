import 'package:fury/src/serializer/collection/collection_serializer_cache.dart';
import 'package:fury/src/serializer/collection/list/list_serializer.dart';
import 'package:fury/src/serializer/serializer.dart';
import 'package:fury/src/serializer/serializer_cache.dart';

final class _DefListSerializerCache extends CollectionSerializerCache{
  static DefListSerializer? _serRef;
  static DefListSerializer? _serNoRef;

  const _DefListSerializerCache();

  @override
  Serializer getSerWithRef(bool writeRef){
    if (writeRef){
      _serRef ??= DefListSerializer._(true);
      return _serRef!;
    } else {
      _serNoRef ??= DefListSerializer._(false);
      return _serNoRef!;
    }
  }
}

final class DefListSerializer extends ListSerializer{

  static const SerializerCache cache = _DefListSerializerCache();
  static const Object obj = Object();
  
  DefListSerializer._(super.writeRef);

  @override
  List newList(int size, bool nullable) {
    return nullable ? List<Object?>.filled(size, null, growable: true) :
        List<Object>.filled(size, obj, growable: true);
  }
}