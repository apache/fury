import '../impl/ref/deser_map_ref_resolver.dart';
import '../impl/ref/deser_no_ref_resolver.dart';

abstract class DeserRefResolver {

  static const DeserRefResolver _noRefResolver = DeserNoRefResolver();

  static DeserRefResolver getOne(bool enableRefTracking) {
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