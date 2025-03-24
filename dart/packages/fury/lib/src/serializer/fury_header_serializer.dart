import 'package:fury/src/config/fury_config.dart';
import 'package:fury/src/const/fury_header_const.dart';
import 'package:fury/src/const/lang.dart';
import 'package:fury/src/exception/deserialization_exception.dart';
import 'package:fury/src/memory/byte_reader.dart';
import 'package:fury/src/memory/byte_writer.dart';

typedef HeaderBrief = ({
  bool isLittleEndian,
  bool isXLang,
  Language peerLang,
  bool oobEnabled,
});

final class FuryHeaderSerializer {
  // singleton
  static final FuryHeaderSerializer _singleton = FuryHeaderSerializer._();
  static FuryHeaderSerializer get I => _singleton;
  FuryHeaderSerializer._();

  HeaderBrief? read(ByteReader br, FuryConfig conf) {
    if (conf.xlangMode){
      int magicNumber = br.readUint16();
      // Note: In AOT mode, assert will be removed, do not put code that affects subsequent logic in assert
      assert(magicNumber == FuryHeaderConst.magicNumber, 'no magic number detected');
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
      throw DeserializationConflictException(
        'XLang:${isXLang? 'true': 'false'}',
        'XLang:${conf.xlangMode? 'true': 'false'}',
      );
    }
    bool oobEnabled = (bitmap & FuryHeaderConst.outOfBandFlag) != 0;
    //TODO: oobEnabled: don't know how to deal with yet.
    // header: peer_lang
    int peerLangInd = br.readInt8();
    if (peerLangInd < Language.peerLangBeginIndex || peerLangInd > Language.peerLangEndIndex){
      throw DeserializationRangeException(peerLangInd, Language.values);
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
    // Next is xWriteRef, handed over to the outside
  }
}
