import 'package:fury_core/src/excep/deser/deser_range_excep.dart';

import '../../fury_core.dart';
import '../config/fury_config.dart';
import '../const/fury_header_const.dart';
import '../excep/dev_excep.dart';
import '../excep/fury_exception.dart';


typedef HeaderBrief = ({
  bool isLittleEndian,
  bool isXLang,
  Language peerLang,
  bool oobEnabled,
});

final class FuryHeaderSer {
  // singleton
  static final FuryHeaderSer _singleton = FuryHeaderSer._();
  static FuryHeaderSer get I => _singleton;
  FuryHeaderSer._();

  HeaderBrief? read(ByteReader br, FuryConfig conf) {
    if (conf.xlangMode){
      int magicNumber = br.readUint16();
      // 注意aot模式下，assert会被去掉，不要把影响后续逻辑的代码放在assert中
      assert(magicNumber == FuryHeaderConst.magicNumber, DevExceps.magicNumberLack(FuryHeaderConst.magicNumber));
    }
    int bitmap = br.readInt8();
    // header: nullFlag
    if ((bitmap & FuryHeaderConst.nullFlag) != 0){
      return null;
    }
    // header: endian
    bool isLittleEndian = (bitmap & FuryHeaderConst.littleEndianFlag) != 0;
    if (!isLittleEndian){
      throw ArgumentError('Non-Little-Endian format detected. Only Little-Endian is supported.');
    }
    // header: xlang
    bool isXLang = (bitmap & FuryHeaderConst.crossLanguageFlag) != 0;
    assert (isXLang, 'Now Fury Dart only supports xlang mode');
    if (conf.xlangMode != isXLang){
      throw FuryException.deserConflict(
          'XLang:${isXLang? 'true': 'false'}',
          'XLang:${conf.xlangMode? 'true': 'false'}'
      );
    }
    bool oobEnabled = (bitmap & FuryHeaderConst.outOfBandFlag) != 0;
    //TODO: oobEnabled: don't know how to deal with yet.
    // header: peer_lang
    int peerLangInd = br.readInt8();
    if (peerLangInd < Language.peerLangBeginIndex || peerLangInd > Language.peerLangEndIndex){
      throw DeserRangeExcep(peerLangInd, Language.values);
    }
    Language.values[peerLangInd];
    // if (!conf.xlangMode && serialPack.peerLang != Language.DART){
    //   throw FuryException.deserConflict(
    //       'PeerLang:${serialPack.peerLang}',
    //       'XLangMode: false'
    //   );
    // }
    return (
      isLittleEndian: isLittleEndian,
      isXLang: isXLang,
      peerLang: Language.values[peerLangInd],
      oobEnabled: oobEnabled,
    );
  }

  void write(ByteWriter bd, bool objNull, FuryConfig conf) {
    assert(conf.xlangMode);
    bd.writeInt16(FuryHeaderConst.magicNumber);
    int bitmap = FuryHeaderConst.littleEndianFlag;
    bitmap |= FuryHeaderConst.crossLanguageFlag;
    if (objNull){
      bitmap |= FuryHeaderConst.nullFlag;
    }
    // callback must be null
    bd.writeInt8(bitmap);
    bd.writeInt8(Language.JAVA.index);
    // 接下来该xWriteRef了，交给外部
  }
}