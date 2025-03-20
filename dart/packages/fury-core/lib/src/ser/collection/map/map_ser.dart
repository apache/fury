import 'package:fury_core/src/deser_pack.dart';
import 'package:fury_core/src/dev_annotation/optimize.dart';
import 'package:fury_core/src/meta/spec_wraps/type_spec_wrap.dart';

import '../../../collection/stack.dart';
import '../../../const/obj_type.dart';
import '../../../memory/byte_reader.dart';
import '../../../memory/byte_writer.dart';
import '../../../ser_pack.dart';
import '../../ser.dart' show Ser;

abstract base class MapSer<T extends Map<Object?,Object?>> extends Ser<Map<Object?,Object?>> {

  const MapSer(bool writeRef): super(ObjType.MAP, writeRef);

  T newMap(int size);

  @override
  T read(ByteReader br, int refId, DeserPack pack) {
    int len = br.readVarUint32Small7();
    T map = newMap(len);
    if (writeRef){
      pack.refResolver.setRefTheLatestId(map);
    }
    TypeSpecWrap? typeWrap = pack.typeWrapStack.peek;
    if (typeWrap == null){
      // 遍历entry
      for (int i = 0; i < len; ++i) {
        Object? key = pack.furyDeser.xReadRefNoSer(br, pack);
        Object? value = pack.furyDeser.xReadRefNoSer(br, pack);
        map[key] = value;
      }
      return map;
    }
    Stack<TypeSpecWrap> typeWrapStack = pack.typeWrapStack;
    TypeSpecWrap keyWrap = typeWrap.param0!;
    TypeSpecWrap valueWrap = typeWrap.param1!;
    Ser? keySer = keyWrap.ser;
    Ser? valueSer = valueWrap.ser;

    if (!keyWrap.hasGenericsParam && !valueWrap.hasGenericsParam){
      // 遍历entry
      for (int i = 0; i < len; ++i) {
        Object? key = _readWithNullableSer(br, keySer, pack);
        Object? value = _readWithNullableSer(br, valueSer, pack);
        map[key] = value;
      }
      return map;
    }
    if (!keyWrap.hasGenericsParam && valueWrap.hasGenericsParam){
      // 遍历entry
      for (int i = 0; i < len; ++i) {
        Object? key = _readWithNullableSer(br, keySer, pack);
        typeWrapStack.push(valueWrap);
        Object? value = _readWithNullableSer(br, valueSer, pack);
        typeWrapStack.pop();
        map[key] = value;
      }
      return map;
    }

    if (keyWrap.hasGenericsParam && !valueWrap.hasGenericsParam){
      // 遍历entry
      for (int i = 0; i < len; ++i) {
        typeWrapStack.push(keyWrap);
        Object? key = _readWithNullableSer(br, keySer, pack);
        typeWrapStack.pop();
        Object? value = _readWithNullableSer(br, valueSer, pack);
        map[key] = value;
      }
      return map;
    }

    for (int i = 0; i < len; ++i) {
      typeWrapStack.push(keyWrap);
      Object? key = _readWithNullableSer(br, keySer, pack);
      typeWrapStack.changeTop(valueWrap);
      Object? value = _readWithNullableSer(br, valueSer, pack);
      typeWrapStack.pop();
      map[key] = value;
    }
    return map;
  }

  /// 这段代码肯呢个看起来有些冗长，但是为了防止在循环中多次进行不必要的判断(因为只进行一次即可)，所以才把外层判断情况分开，导致代码看起来有些长
  @override
  void write(ByteWriter bw, covariant T v, SerPack pack) {
    bw.writeVarUint32Small7(v.length);
    TypeSpecWrap? typeWrap = pack.typeWrapStack.peek;
    if (typeWrap == null){
      // 遍历entry
      for (var entry in v.entries) {
        Object? key = entry.key;
        Object? value = entry.value;
        pack.furySer.xWriteRefNoSer(bw, key, pack);
        pack.furySer.xWriteRefNoSer(bw, value, pack);
      }
      return;
    }
    var typeWrapStack = pack.typeWrapStack;
    TypeSpecWrap keyWrap = typeWrap.param0!;
    TypeSpecWrap valueWrap = typeWrap.param1!;
    Ser? keySer = keyWrap.ser;
    Ser? valueSer = valueWrap.ser;

    if (!keyWrap.hasGenericsParam && !valueWrap.hasGenericsParam){
      // 遍历entry
      for (var entry in v.entries) {
        Object? key = entry.key;
        Object? value = entry.value;
        _writeWithNullableSer(bw, key, keySer, pack);
        _writeWithNullableSer(bw, value, valueSer, pack);
      }
      return;
    }

    if (keyWrap.hasGenericsParam && !valueWrap.hasGenericsParam){
      // 遍历entry
      for (var entry in v.entries) {
        Object? key = entry.key;
        Object? value = entry.value;
        typeWrapStack.push(keyWrap);
        _writeWithNullableSer(bw, key, keySer, pack);
        typeWrapStack.pop();
        _writeWithNullableSer(bw, value, valueSer, pack);
      }
      return;
    }

    if (!keyWrap.hasGenericsParam && valueWrap.hasGenericsParam){
      // 遍历entry
      for (var entry in v.entries) {
        Object? key = entry.key;
        Object? value = entry.value;
        _writeWithNullableSer(bw, key, keySer, pack);
        typeWrapStack.push(valueWrap);
        _writeWithNullableSer(bw, value, valueSer, pack);
        typeWrapStack.pop();
      }
      return;
    }
    // 遍历entry
    for (var entry in v.entries) {
      Object? key = entry.key;
      Object? value = entry.value;
      typeWrapStack.push(keyWrap);
      _writeWithNullableSer(bw, key, keySer, pack);
      typeWrapStack.changeTop(valueWrap);
      _writeWithNullableSer(bw, value, valueSer, pack);
      typeWrapStack.pop();
    }
  }

  @inline
  void _writeWithNullableSer(ByteWriter bw, Object? v, Ser? ser, SerPack pack){
    ser == null ?
    pack.furySer.xWriteRefNoSer(bw, v, pack) :
    pack.furySer.xWriteRefWithSer(bw, ser, v, pack);
  }

  @inline
  Object? _readWithNullableSer(ByteReader br, Ser? ser, DeserPack pack){
    return ser == null ?
    pack.furyDeser.xReadRefNoSer(br, pack) :
    pack.furyDeser.xReadRefWithSer(br, ser, pack);
  }
}