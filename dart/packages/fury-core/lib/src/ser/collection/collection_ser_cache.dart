import 'package:fury_core/src/ser/ser.dart' show Ser;

import '../../config/fury_config.dart';
import '../ser_cache.dart';

abstract base class CollectionSerCache extends SerCache{
  const CollectionSerCache();

  @override
  Ser getSer(FuryConfig conf,){
    return getSerWithRef(conf.refTracking);
  }

  Ser getSerWithRef(bool writeRef);
}