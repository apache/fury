import 'package:fury_core/src/ser/collection/collection_ser_cache.dart';
import 'package:fury_core/src/ser/collection/list/list_ser.dart';

import '../../ser.dart' show Ser;
import '../../ser_cache.dart';

final class _DefListSerCache extends CollectionSerCache{
  static DefListSer? _serRef;
  static DefListSer? _serNoRef;

  const _DefListSerCache();

  @override
  Ser getSerWithRef(bool writeRef){
    if (writeRef){
      _serRef ??= DefListSer._(true);
      return _serRef!;
    } else {
      _serNoRef ??= DefListSer._(false);
      return _serNoRef!;
    }
  }
}


final class DefListSer extends ListSer{

  static const SerCache cache = _DefListSerCache();
  static const Object obj = Object();
  
  DefListSer._(super.writeRef);

  @override
  List newList(int size, bool nullable) {
    return nullable ? List<Object?>.filled(size, null, growable: true) :
        List<Object>.filled(size, obj, growable: true);
  }
}