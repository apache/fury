import 'package:fury/src/config/fury_config.dart';
import 'package:fury/src/serializer/serializer.dart';
import 'package:fury/src/serializer/serializer_cache.dart';

abstract base class ArraySerializerCache extends SerializerCache{
  const ArraySerializerCache();

  @override
  Serializer getSer(FuryConfig conf,){
    return getSerWithRef(conf.refTracking);
  }

  Serializer getSerWithRef(bool writeRef);
}

abstract base class ArraySerializer<T> extends Serializer<List<T>> {
  const ArraySerializer(super.type, super.writeRef);
}