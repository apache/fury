import 'package:fury/src/memory/byte_writer.dart';
import 'package:fury/src/meta/meta_string_byte.dart';
import 'package:fury/src/resolver/ms_writing_resolver.dart';

final class MsWritingResolverImpl extends MsWritingResolver{
  
  int _dynamicWriteStrId = 0;

  final Map<int, int> _memHash2Id = {};
  
  @override
  void writeMsb(ByteWriter bw, MetaStringBytes msb) {
    int idenHash = identityHashCode(msb);
    int? id = _memHash2Id[idenHash];
    if(id != null){
      bw.writeVarUint32Small7( ((id + 1) << 1) | 1 );
      return;
    }
    _memHash2Id[idenHash] = _dynamicWriteStrId;
    ++_dynamicWriteStrId;
    int bytesLen = msb.length;
    bw.writeVarUint32Small7(bytesLen << 1);
    if (bytesLen > smallStringThreshold){
      bw.writeInt64(msb.hashCode);
    }else {
      bw.writeInt8(msb.encoding.id);
    }
    bw.writeBytes(msb.bytes);
  }
}