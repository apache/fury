import 'package:fury_core/src/ser/ser.dart' show Ser;
import 'package:fury_core/src/ser/ser_cache.dart';

import '../../config/fury_config.dart';

abstract base class PrimitiveSerCache extends SerCache{

  const PrimitiveSerCache();

  @override
  Ser getSer(FuryConfig conf,){
    // 目前Primitive类型的Ser只有写入Ref和不写入Ref两种，所以这里只缓存两种
    bool writeRef = conf.refTracking && !conf.basicTypesRefIgnored;
    return getSerWithRef(writeRef);
  }

  Ser getSerWithRef(bool writeRef);
}