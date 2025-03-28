import 'package:fury/src/config/fury_config.dart';
import 'package:fury/src/serializer/serializer.dart';
import 'package:fury/src/serializer/serializer_cache.dart';

abstract base class CollectionSerializerCache extends SerializerCache{
  const CollectionSerializerCache();

  @override
  Serializer getSer(FuryConfig conf,){
    return getSerWithRef(conf.refTracking);
  }

  Serializer getSerWithRef(bool writeRef);
}