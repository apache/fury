import 'dart:collection';

import 'package:fury_core/src/ser/collection/collection_ser_cache.dart';
import 'package:fury_core/src/ser/collection/set/set_ser.dart';

import '../../ser.dart' show Ser;
import '../../ser_cache.dart';

final class _LinkedHashSetSerCache extends CollectionSerCache{
  static LinkedHashSetSer? _serRef;
  static LinkedHashSetSer? _serNoRef;

  const _LinkedHashSetSerCache();

  @override
  Ser getSerWithRef(bool writeRef){
    if (writeRef){
      _serRef ??= LinkedHashSetSer._(true);
      return _serRef!;
    } else {
      _serNoRef ??= LinkedHashSetSer._(false);
      return _serNoRef!;
    }
  }
}


final class LinkedHashSetSer extends SetSer{

  static const SerCache cache = _LinkedHashSetSerCache();
  static const Object obj = Object();

  LinkedHashSetSer._(super.writeRef);

  @override
  Set newSet(bool nullable) {
    return nullable ? LinkedHashSet<Object?>() : LinkedHashSet<Object>();
  }
}