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

import 'package:analyzer/dart/element/element.dart';
import 'package:build/build.dart';
import 'package:source_gen/source_gen.dart';
import 'package:fury/src/annotation/fury_object.dart';
import 'package:fury/src/codegen/analyze/analysis_type_identifier.dart';
import 'package:fury/src/codegen/analyze/analyzer.dart';
import 'package:fury/src/codegen/exception/annotation_exception.dart';
import 'package:fury/src/codegen/meta/gen_export.dart';
import 'package:fury/src/annotation/fury_enum.dart';
import 'package:fury/src/annotation/fury_class.dart';

class ObjSpecGenerator extends GeneratorForAnnotation<FuryObject> {

  static int? enumElementId;
  static int? classElementId;

  @override
  Future<String> generateForAnnotatedElement(
    Element element,
    ConstantReader annotation,
    BuildStep buildStep
  ) async {
    Element anno = annotation.objectValue.type!.element!;
    late GenExport spec;
    late bool enumOrClass;
    if (enumElementId != null){
      enumOrClass = anno.id == enumElementId;
    }else{
      if (classElementId != null) {
        enumOrClass = anno.id != classElementId;
      }else{
        if (anno.name == 'FuryClass'){
          enumOrClass = false;
          classElementId = anno.id;
          AnalysisTypeIdentifier.giveFuryClassId(anno.id);
        }else{
          enumOrClass = true;
          enumElementId = anno.id;
          AnalysisTypeIdentifier.giveFuryEnumId(anno.id);
        }
      }
    }

    if (enumOrClass){
      if (element is! EnumElement) {
        throw InvalidAnnotationTargetException(
          FuryEnum.name,
          element.displayName,
          FuryEnum.targets,
        );
      }
      spec = Analyzer.enumAnalyzer.analyze(element);
    }else {
      if (element is! ClassElement) {
        throw InvalidAnnotationTargetException(
          FuryClass.name,
          element.displayName,
          FuryEnum.targets,
        );
      }
      spec = Analyzer.classAnalyzer.analyze(element);
    }

    StringBuffer buf = StringBuffer();
    spec.genCode(buf);
    return buf.toString();
  }
}