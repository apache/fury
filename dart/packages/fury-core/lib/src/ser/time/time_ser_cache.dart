import 'package:fury_core/src/ser/ser.dart' show Ser;
import 'package:fury_core/src/ser/ser_cache.dart';

import '../../config/fury_config.dart';

abstract base class TimeSerCache extends SerCache{

  const TimeSerCache();

  @override
  Ser getSer(FuryConfig conf, [Type? type]){
    // 目前Primitive类型的Ser只有写入Ref和不写入Ref两种，所以这里只缓存两种
    bool writeRef = conf.refTracking && !conf.timeRefIgnored;
    return getSerWithRef(writeRef);
  }

  Ser getSerWithRef(bool writeRef);
}