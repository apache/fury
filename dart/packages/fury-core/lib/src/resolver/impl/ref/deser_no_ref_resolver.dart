import '../../ref/deser_ref_resolver.dart';
class DeserNoRefResolver implements DeserRefResolver{

  const DeserNoRefResolver();

  // @override
  // void appendRef(Object obj) {}

  @override
  Object getObj(int refId) {
    throw UnimplementedError("NoRefResolver does not support getObj");
  }

  @override
  int reserveId() {
    return 0; // nothing
  }

  @override
  void setRefTheLatestId(Object o) {
    // do nothing
  }

  @override
  void setRef(int refId, Object o) {
    // do nothing
  }

  // @override
  // int reserveIdSetMuchLatter() {
  //   return 0; // nothing
  // }
  //
  // @override
  // void setRefForLaterRefId(Object o) {
  //   // do nothing
  // }

}