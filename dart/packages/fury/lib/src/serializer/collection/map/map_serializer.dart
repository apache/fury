import 'package:fury/src/deserializer_pack.dart';
import 'package:fury/src/dev_annotation/optimize.dart';
import 'package:fury/src/meta/spec_wraps/type_spec_wrap.dart';
import 'package:fury/src/collection/stack.dart';
import 'package:fury/src/const/obj_type.dart';
import 'package:fury/src/memory/byte_reader.dart';
import 'package:fury/src/memory/byte_writer.dart';
import 'package:fury/src/serializer_pack.dart';
import 'package:fury/src/serializer/serializer.dart';

abstract base class MapSerializer<T extends Map<Object?,Object?>> extends Serializer<Map<Object?,Object?>> {

  const MapSerializer(bool writeRef): super(ObjType.MAP, writeRef);

  T newMap(int size);

  @override
  T read(ByteReader br, int refId, DeserializerPack pack) {
    int len = br.readVarUint32Small7();
    T map = newMap(len);
    if (writeRef){
      pack.refResolver.setRefTheLatestId(map);
    }
    TypeSpecWrap? typeWrap = pack.typeWrapStack.peek;
    if (typeWrap == null){
      // Traverse entries
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
    Serializer? keySer = keyWrap.ser;
    Serializer? valueSer = valueWrap.ser;

    if (!keyWrap.hasGenericsParam && !valueWrap.hasGenericsParam){
      // Traverse entries
      for (int i = 0; i < len; ++i) {
        Object? key = _readWithNullableSer(br, keySer, pack);
        Object? value = _readWithNullableSer(br, valueSer, pack);
        map[key] = value;
      }
      return map;
    }
    if (!keyWrap.hasGenericsParam && valueWrap.hasGenericsParam){
      // Traverse entries
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
      // Traverse entries
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

  /// This code may look a bit lengthy, but to avoid unnecessary checks multiple times in the loop
  /// (since it only needs to be done once), the outer judgment cases are separated, making the code look a bit long
  @override
  void write(ByteWriter bw, covariant T v, SerPack pack) {
    bw.writeVarUint32Small7(v.length);
    TypeSpecWrap? typeWrap = pack.typeWrapStack.peek;
    if (typeWrap == null){
      // Traverse entries
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
    Serializer? keySer = keyWrap.ser;
    Serializer? valueSer = valueWrap.ser;

    if (!keyWrap.hasGenericsParam && !valueWrap.hasGenericsParam){
      // Traverse entries
      for (var entry in v.entries) {
        Object? key = entry.key;
        Object? value = entry.value;
        _writeWithNullableSer(bw, key, keySer, pack);
        _writeWithNullableSer(bw, value, valueSer, pack);
      }
      return;
    }

    if (keyWrap.hasGenericsParam && !valueWrap.hasGenericsParam){
      // Traverse entries
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
      // Traverse entries
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
    // Traverse entries
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
  void _writeWithNullableSer(ByteWriter bw, Object? v, Serializer? ser, SerPack pack){
    ser == null ?
    pack.furySer.xWriteRefNoSer(bw, v, pack) :
    pack.furySer.xWriteRefWithSer(bw, ser, v, pack);
  }

  @inline
  Object? _readWithNullableSer(ByteReader br, Serializer? ser, DeserializerPack pack){
    return ser == null ?
    pack.furyDeser.xReadRefNoSer(br, pack) :
    pack.furyDeser.xReadRefWithSer(br, ser, pack);
  }
}
