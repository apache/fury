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

import 'package:meta/meta.dart';

import 'package:fury/src/codec/entity/str_stat.dart';
import 'package:fury/src/codec/meta_string_codecs.dart';
import 'package:fury/src/codec/meta_string_encoding.dart';
import 'package:fury/src/meta/meta_string.dart';
import 'package:fury/src/util/char_util.dart';

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
    // The encoding array is very small, so the List's contains method is used. If more encodings need to be supported in the future, consider using a Set.
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