import 'package:fury/src/memory/byte_writer.dart';
import 'package:fury/src/meta/spec_wraps/type_spec_wrap.dart';
import 'package:fury/src/serializer/serializer.dart';
import 'package:fury/src/serializer_pack.dart';

abstract base class IterableSerializer extends Serializer<Iterable> {

  const IterableSerializer(super.objType, super.writeRef);

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
      Serializer? ser = elemWrap.ser;
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