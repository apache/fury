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

import 'package:analyzer/dart/constant/value.dart';
import 'package:analyzer/dart/element/element.dart';
import 'package:fury/src/codegen/analyze/analysis_type_identifier.dart';
import 'package:fury/src/codegen/analyze/annotation/location_level_ensure.dart';
import 'package:fury/src/codegen/const/location_level.dart';
import 'package:fury/src/codegen/entity/location_mark.dart';
import 'package:fury/src/codegen/exception/annotation_exception.dart';
import 'package:fury/src/annotation/fury_key.dart';

class KeyAnnotationAnalyzer {

  const KeyAnnotationAnalyzer();

  // Currently, FuryKey will analyze only once at a run, so there is no need to use a caching mechanism.
  FuryKey analyze(
    List<ElementAnnotation> metadata,
    @LocationEnsure(LocationLevel.fieldLevel) LocationMark locationMark,
  ){
    assert(locationMark.ensureFieldLevel);
    DartObject? anno;
    late ClassElement annoClsElement;
    bool getMeta = false;
    for (ElementAnnotation annoElement in metadata){
      anno = annoElement.computeConstantValue()!;
      annoClsElement = anno.type!.element as ClassElement;
      if (AnalysisTypeIdentifier.isFuryKey(annoClsElement)){
        if (getMeta){
          throw DuplicatedAnnotationException(
            FuryKey.name,
            locationMark.fieldName!,
            locationMark.clsLocation,
          );
        }
        getMeta = true;
        // 目前只处理FuryKey
      }
    }
    // If there is no annotation, both includeFromFury and includeToFury default to true.
    bool includeFromFury = true;
    bool includeToFury = true;
    if (anno != null){
      includeFromFury = anno.getField("includeFromFury")!.toBoolValue()!;
      includeToFury = anno.getField("includeToFury")!.toBoolValue()!;
      // serializeToVar = anno.getField("serializeTo")?.variable;
      // deserializeFromVar = anno.getField("deserializeFrom")?.variable;
      // if (serializeToVar != null){
      //   serializeTo = FuryType.fromString(serializeToVar.name)!;
      // }
      // if (deserializeFromVar != null){
      //   deserializeFrom = FuryType.fromString(deserializeFromVar.name)!;
      // }
      // targetType = (anno.getField("targetType")!.variable.enclosingElement3 as EnumElement).fields[1].;
    }
    FuryKey furyKey = FuryKey(
      // serializeTo: serializeTo,
      // deserializeFrom: deserializeFrom,
      includeFromFury: includeFromFury,
      includeToFury: includeToFury,
    );
    return furyKey;
  }
}