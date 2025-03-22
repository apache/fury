
import 'package:fury_core/src/codec/meta_string_codecs.dart';
import 'package:fury_core/src/codec/meta_string_encoding.dart';
import 'package:fury_core/src/meta/meta_string.dart';
import 'package:meta/meta.dart';

import '../util/char_util.dart';
import 'entity/str_stat.dart';

abstract base class MetaStringEncoder extends MetaStringCodecs {
  const MetaStringEncoder(super.specialChar1, super.specialChar2);

  // MetaString encode(String input, MetaStrEncoding encoding);
  MetaString encodeByAllowedEncodings(String input, List<MetaStrEncoding> encodings);

  StrStat _computeStrStat(String input){
    bool canLUDS = true;
    bool canLS = true;
    int digitCount = 0;
    int upperCount = 0;
    for (var c in input.codeUnits){
      if (canLUDS && !CharUtil.isLUD(c) && c != specialChar1 && c != specialChar2) canLUDS = false;
      if (canLS && !CharUtil.isLS(c)) canLS = false;
      if (CharUtil.digit(c)) ++digitCount;
      if (CharUtil.upper(c)) ++upperCount;
    }
    return StrStat(digitCount, upperCount, canLUDS, canLS,);
  }

  @protected
  MetaStrEncoding decideEncoding(String input, List<MetaStrEncoding> encodings) {
    List<bool> flags = List.filled(MetaStrEncoding.values.length, false);
    for (var e in encodings) {
      flags[e.index] = true;
    }
    // encoding数组极小，所及使用List的contains方法,如果之后需要支持更多的encoding，可以考虑使用Set
    if(input.isEmpty && flags[MetaStrEncoding.ls.index]){
      return MetaStrEncoding.ls;
    }
    StrStat stat = _computeStrStat(input);
    if (stat.canLS && flags[MetaStrEncoding.ls.index]){
      return MetaStrEncoding.ls;
    }
    if (stat.canLUDS){
      if (stat.digitCount != 0 && flags[MetaStrEncoding.luds.index]){
        return MetaStrEncoding.luds;
      }
      if (stat.upperCount == 1 && CharUtil.upper(input.codeUnitAt(0)) && flags[MetaStrEncoding.ftls.index]){
        return MetaStrEncoding.ftls;
      }
      if (
        ((input.length + stat.upperCount) * 5 < input.length * 6) &&
        flags[MetaStrEncoding.atls.index]
        ) {
        return MetaStrEncoding.atls;
      }
      if (flags[MetaStrEncoding.luds.index]){
        return MetaStrEncoding.luds;
      }
    }
    return MetaStrEncoding.utf8;
  }
}