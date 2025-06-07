/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

import 'package:fory/src/config/fory_config.dart';
import 'package:fory/src/const/fory_header_const.dart';
import 'package:fory/src/const/lang.dart';
import 'package:fory/src/exception/deserialization_exception.dart';
import 'package:fory/src/memory/byte_reader.dart';
import 'package:fory/src/memory/byte_writer.dart';

typedef HeaderBrief = ({
  bool isLittleEndian,
  bool isXLang,
  Language peerLang,
  bool oobEnabled,
});

final class ForyHeaderSerializer {
  // singleton
  static final ForyHeaderSerializer _singleton = ForyHeaderSerializer._();
  static ForyHeaderSerializer get I => _singleton;
  ForyHeaderSerializer._();

  HeaderBrief? read(ByteReader br, ForyConfig conf) {
    int magicNumber = br.readUint16();
    // Note: In AOT mode, assert will be removed, do not put code that affects subsequent logic in assert
    assert(magicNumber == ForyHeaderConst.magicNumber, 'no magic number detected');
    int bitmap = br.readInt8();
    // header: nullFlag
    if ((bitmap & ForyHeaderConst.nullFlag) != 0){
      return null;
    }
    // header: endian
    bool isLittleEndian = (bitmap & ForyHeaderConst.littleEndianFlag) != 0;
    if (!isLittleEndian){
      throw ArgumentError('Non-Little-Endian format detected. Only Little-Endian is supported.');
    }
    // header: xlang
    bool isXLang = (bitmap & ForyHeaderConst.crossLanguageFlag) != 0;
    assert (isXLang, 'Now Fory Dart only supports xlang mode');
    bool oobEnabled = (bitmap & ForyHeaderConst.outOfBandFlag) != 0;
    //TODO: oobEnabled unsupported yet.
    // header: peer_lang
    int peerLangInd = br.readInt8();
    if (peerLangInd < Language.peerLangBeginIndex || peerLangInd > Language.peerLangEndIndex){
      throw DeserializationRangeException(peerLangInd, Language.values);
    }
    Language.values[peerLangInd];
    // if (!conf.xlangMode && serialPack.peerLang != Language.DART){
    //   throw ForyException.deserConflict(
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

  void write(ByteWriter bd, bool objNull, ForyConfig conf) {
    bd.writeInt16(ForyHeaderConst.magicNumber);
    int bitmap = ForyHeaderConst.littleEndianFlag;
    bitmap |= ForyHeaderConst.crossLanguageFlag;
    if (objNull){
      bitmap |= ForyHeaderConst.nullFlag;
    }
    // callback must be null
    bd.writeInt8(bitmap);
    bd.writeInt8(Language.dart.index);
    // Next is xWriteRef, handed over to the outside
  }
}
