import 'package:fury_core/src/deser_pack.dart';
import 'package:fury_core/src/meta/spec_wraps/type_spec_wrap.dart';
import 'package:fury_core/src/ser/collection/iterable_ser.dart';

import '../../../const/obj_type.dart';
import '../../../memory/byte_reader.dart';
import '../../ser.dart' show Ser;

abstract base class ListSer extends IterableSer {

  const ListSer(bool writeRef): super(ObjType.LIST, writeRef);
  
  List newList(int size, bool nullable);

  @override
  List read(ByteReader br, int refId, DeserPack pack) {
    int num = br.readVarUint32Small7();
    TypeSpecWrap? elemWrap = pack.typeWrapStack.peek?.param0;
    List list = newList(
      num,
      elemWrap == null || elemWrap.nullable,
    );
    if (writeRef){
      pack.refResolver.setRefTheLatestId(list);
    }
    if (elemWrap == null){
      for (int i = 0; i < num; ++i) {
        Object? o = pack.furyDeser.xReadRefNoSer(br, pack);
        list[i] = o;
      }
      return list;
    }
    if (elemWrap.hasGenericsParam){
      pack.typeWrapStack.push(elemWrap);
    }
    if (!elemWrap.certainForSer){
      for (int i = 0; i < num; ++i) {
        Object? o = pack.furyDeser.xReadRefNoSer(br, pack);
        list[i] = o;
      }
    }else {
      Ser? ser = elemWrap.ser;
      if (ser == null){
        for (int i = 0; i < num; ++i) {
          Object? o = pack.furyDeser.xReadRefNoSer(br, pack);
          list[i] = o;
        }
      }else{
        for (int i = 0; i < num; ++i) {
          Object? o = pack.furyDeser.xReadRefWithSer(br, ser, pack);
          list[i] = o;
        }
      }
    }
    if (elemWrap.hasGenericsParam){
      pack.typeWrapStack.pop();
    }
    return list;
  }
}