import 'package:fury/src/config/fury_config.dart';
import 'package:fury/src/serializer/serializer.dart';
import 'package:fury/src/serializer/serializer_cache.dart';

abstract base class PrimitiveSerializerCache extends SerializerCache{

  const PrimitiveSerializerCache();

  @override
  Serializer getSer(FuryConfig conf,){
    // Currently, there are only two types of Ser for primitive types: one that write a reference
    // and one that does not, so only these two are cached here.
    bool writeRef = conf.refTracking && !conf.basicTypesRefIgnored;
    return getSerWithRef(writeRef);
  }

  Serializer getSerWithRef(bool writeRef);
}
