import 'package:fury_core/src/ser/ser.dart' show Ser;

import '../../config/fury_config.dart';
import '../ser_cache.dart';

abstract base class ArraySerCache extends SerCache{
  const ArraySerCache();

  @override
  Ser getSer(FuryConfig conf,){
    return getSerWithRef(conf.refTracking);
  }

  Ser getSerWithRef(bool writeRef);
}

abstract base class ArraySer<T> extends Ser<List<T>> {

  const ArraySer(super.type, super.writeRef);
}