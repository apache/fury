import 'package:fury_core/src/meta/spec_wraps/type_spec_wrap.dart';

import '../../../fury_core.dart';
import '../../ser_pack.dart';

abstract base class IterableSer extends Ser<Iterable> {

  const IterableSer(super.objType, super.writeRef);

  @override
  void write(ByteWriter bw, Iterable v, SerPack pack) {
    bw.writeVarUint32Small7(v.length);
    TypeSpecWrap? elemWrap = pack.typeWrapStack.peek?.param0;
    if (elemWrap == null){
      for (var o in v) {
        pack.furySer.xWriteRefNoSer(bw, o, pack);
      }
      return;
    }
    if (elemWrap.hasGenericsParam){
      pack.typeWrapStack.push(elemWrap);
    }
    if (!elemWrap.certainForSer){
      for (var o in v) {
        pack.furySer.xWriteRefNoSer(bw, o, pack);
      }
    }else {
      Ser? ser = elemWrap.ser;
      if (ser == null){
        for (var o in v) {
          pack.furySer.xWriteRefNoSer(bw, o, pack);
        }
      }else{
        for (var o in v) {
          pack.furySer.xWriteRefWithSer(bw, ser, o, pack);
        }
      }
    }
    if (elemWrap.hasGenericsParam){
      pack.typeWrapStack.pop();
    }
  }
}