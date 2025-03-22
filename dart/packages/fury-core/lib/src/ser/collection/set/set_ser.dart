/// 实际上我们的序列化的过程中，List与Set是没有区别的，但是Dart中List和Set没有一个类似的Collection的父类型
/// 他们随谈都实现Iterable接口，这使得可以使用统一的方法进行读
/// 单不存在有任何上层的add方法，没有办法使用统一的方法写，
/// 所以即使大体逻辑相同，为了不破坏Serializer的继承结构，这里还是要单独实现，这可能会引入重复的代码
///
library;

import 'package:fury_core/src/deser_pack.dart';
import 'package:fury_core/src/meta/spec_wraps/type_spec_wrap.dart';
import 'package:fury_core/src/ser/collection/iterable_ser.dart';

import '../../../const/obj_type.dart';
import '../../../memory/byte_reader.dart';
import '../../ser.dart' show Ser;

abstract base class SetSer extends IterableSer {

  const SetSer(bool writeRef): super(ObjType.SET, writeRef);
  
  Set newSet(bool nullable);

  @override
  Set read(ByteReader br, int refId, DeserPack pack) {
    int num = br.readVarUint32Small7();
    TypeSpecWrap? elemWrap = pack.typeWrapStack.peek?.param0;
    Set set = newSet(
      elemWrap == null || elemWrap.nullable,
    );
    if (writeRef){
      pack.refResolver.setRefTheLatestId(set);
    }
    if (elemWrap == null){
      for (int i = 0; i < num; ++i) {
        Object? o = pack.furyDeser.xReadRefNoSer(br, pack);
        set.add(o);
      }
      return set;
    }
    if (elemWrap.hasGenericsParam){
      pack.typeWrapStack.push(elemWrap);
    }
    if (!elemWrap.certainForSer){
      for (int i = 0; i < num; ++i) {
        Object? o = pack.furyDeser.xReadRefNoSer(br, pack);
        set.add(o);
      }
    }else {
      Ser? ser = elemWrap.ser;
      if (ser == null){
        for (int i = 0; i < num; ++i) {
          Object? o = pack.furyDeser.xReadRefNoSer(br, pack);
          set.add(o);
        }
      }else{
        for (int i = 0; i < num; ++i) {
          Object? o = pack.furyDeser.xReadRefWithSer(br, ser, pack);
          set.add(o);
        }
      }
    }
    if (elemWrap.hasGenericsParam){
      pack.typeWrapStack.pop();
    }
    return set;
  }
}