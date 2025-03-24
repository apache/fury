import 'package:fury/src/resolver/ref/deser_ref_resolver.dart';

class DeserMapRefResolver implements DeserRefResolver{

  final List<Object?> _refs = [];

  int _lastRefId = -1;

  @override
  Object getObj(int refId) {
    assert (_refs.length > refId);
    return _refs[refId]!;
  }

  @override
  int reserveId() {
    ++_lastRefId;
    _refs.add(null);
    return _lastRefId;
  }

  @override
  void setRefTheLatestId(Object o) {
    _refs[_lastRefId] = o;
  }

  @override
  void setRef(int refId, Object o) {
    _refs[refId] = o;
  }

  // @override
  // int reserveIdSetMuchLatter() {
  //   assert(_refIdWillSetLater == null);
  //   _refIdWillSetLater = reserveId();
  //   return _refIdWillSetLater!;
  // }
  //
  // @override
  // void setRefForLaterRefId(Object o) {
  //   assert(_refIdWillSetLater != null);
  //   _refs[_refIdWillSetLater!] = o;
  //   _refIdWillSetLater = null;
  // }
}