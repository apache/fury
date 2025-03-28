import 'package:fury/src/config/fury_config.dart';
import 'package:fury/src/serializer/serializer.dart';
import 'package:fury/src/serializer/serializer_cache.dart';

abstract base class TimeSerializerCache extends SerializerCache{

  const TimeSerializerCache();

  @override
  Serializer getSer(FuryConfig conf, [Type? type]){
    // Currently, there are only two types of Serialization for primitive types:
    // with ref and without ref. So only these two are cached here.
    bool writeRef = conf.refTracking && !conf.timeRefIgnored;
    return getSerWithRef(writeRef);
  }

  Serializer getSerWithRef(bool writeRef);
}
