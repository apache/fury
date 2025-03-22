import 'package:fury_core/src/const/ref_flag.dart';

import '../impl/ref/ser_map_resolver.dart';
import '../impl/ref/ser_no_ref_resolver.dart';

typedef SerRefRes = ({RefFlag refFlag, int? refId});

abstract base class SerRefResolver {

  static SerRefResolver noRefResolver = SerNoRefResolver();

  static SerRefResolver getOne(bool enableRefTracking) {
    return enableRefTracking ? SerMapRefResolver() : noRefResolver;
  }

  const SerRefResolver();

  SerRefRes getRefId(Object? obj);

  RefFlag getRefFlag(Object? obj){
    throw UnimplementedError('getRefFlag not implemented');
  }
}