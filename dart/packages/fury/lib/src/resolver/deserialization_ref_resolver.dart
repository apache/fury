import 'package:fury/src/resolver/impl/deser_map_ref_resolver.dart';
import 'package:fury/src/resolver/impl/deser_no_ref_resolver.dart';

abstract class DeserializationRefResolver {

  static const DeserializationRefResolver _noRefResolver = DeserNoRefResolver();

  static DeserializationRefResolver getOne(bool enableRefTracking) {
    return enableRefTracking ? DeserMapRefResolver() : _noRefResolver;
  }

  Object getObj(int refId);

  // void appendRef(Object obj);

  int reserveId();

  // int reserveIdSetMuchLatter();

  void setRefTheLatestId(Object o);

  void setRef(int refId, Object o);

  // void setRefForLaterRefId(Object o);
}