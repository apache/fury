import 'dart:typed_data';

import 'package:fury_core/fury_core.dart';
import 'package:fury_core/src/excep/deser/deser_unsupported_feature.dart';
import 'package:fury_core/src/util/string_util.dart';

import '../../config/fury_config.dart';
import '../../deser_pack.dart';
import '../../ser_pack.dart';
import '../ser_cache.dart';

enum _StrCode{
  latin1(0),
  utf16(1);

  final int id;
  const _StrCode(this.id);
}

final class _StringSerCache extends SerCache{
  static StringSer? serRef;
  static StringSer? serNoRef;

  const _StringSerCache();

  @override
  Ser getSer(FuryConfig conf,){
    // 目前Primitive类型的Ser只有写入Ref和不写入Ref两种，所以这里只缓存两种
    bool writeRef = conf.refTracking && !conf.stringRefIgnored;
    return getSerWithRef(writeRef);
  }
  Ser getSerWithRef(bool writeRef) {
    if (writeRef){
      serRef ??= StringSer(true);
      return serRef!;
    } else {
      serNoRef ??= StringSer(false);
      return serNoRef!;
    }
  }
}


final class StringSer extends Ser<String>{

  static const SerCache cache = _StringSerCache();

  StringSer(bool writeRef): super(ObjType.STRING, writeRef);

  @override
  String read(ByteReader br, int refId, DeserPack pack){
    int header = br.readVarUint36Small();
    int coder = header & 3;
    int byteNum = header >>> 2;
    if (coder == _StrCode.latin1.id){
      return _readLatin1(br, byteNum);
    }else if (coder == _StrCode.utf16.id) {
      return _readUtf16(br, byteNum);
    }
    throw DeserUnsupportedFeatureExcep(coder, _StrCode.values, 'String Coder');
  }

  @override
  void write(ByteWriter bw, String v, SerPack pack){
    if (StringUtil.hasNonLatin(v)){
      _writeUtf16(bw, v);
      return;
    }
    _writeLatin1(bw, v);
  }

  String _readLatin1(ByteReader br, int byteNum){
    Uint8List bytesView = br.readBytesView(byteNum);
    return String.fromCharCodes(bytesView);
  }

  String _readUtf16(ByteReader br, int byteNum){
    Uint16List bytes = br.readCopyUint16List(byteNum);
    return String.fromCharCodes(bytes);
  }

  void _writeLatin1(ByteWriter bw, String v) {
    bw.writeVarUint36Small( (v.length << 2) | _StrCode.latin1.id);
    bw.writeBytes(v.codeUnits);
  }

  void _writeUtf16(ByteWriter bw, String v) {
    // 这里(v.length * 2)  << 2 == (v.length  << 3);
    bw.writeVarUint36Small( (v.length  << 3) | _StrCode.utf16.id);
    bw.writeBytes(
      Uint16List.fromList(v.codeUnits).buffer.asUint8List(),
    );
  }
}