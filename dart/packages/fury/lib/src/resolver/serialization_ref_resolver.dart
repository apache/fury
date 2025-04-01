import 'package:fury/src/const/ref_flag.dart';
import 'package:fury/src/resolver/impl/ser_map_resolver.dart';
import 'package:fury/src/resolver/impl/ser_no_ref_resolver.dart';

typedef SerializationRefMeta = ({RefFlag refFlag, int? refId});

abstract base class SerializationRefResolver {

  static SerializationRefResolver noRefResolver = SerNoRefResolver();

  static SerializationRefResolver getOne(bool enableRefTracking) {
    return enableRefTracking ? SerMapRefResolver() : noRefResolver;
  }

  const SerializationRefResolver();

  SerializationRefMeta getRefId(Object? obj);

  RefFlag getRefFlag(Object? obj){
    throw UnimplementedError('getRefFlag not implemented');
  }
}