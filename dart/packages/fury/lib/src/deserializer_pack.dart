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

import 'package:fury/src/deserialize_coordinator.dart';
import 'package:fury/src/meta/spec_wraps/type_spec_wrap.dart';
import 'package:fury/src/resolver/deserialization_ref_resolver.dart';
import 'package:fury/src/resolver/xtype_resolver.dart';
import 'package:fury/src/serializer/fury_header_serializer.dart';
import 'package:fury/src/pack.dart';
import 'package:fury/src/collection/stack.dart';

final class DeserializerPack extends Pack{
   final HeaderBrief header;

   final DeserializeCoordinator furyDeser;

   final DeserializationRefResolver refResolver;
   final XtypeResolver xtypeResolver;

   final Stack<TypeSpecWrap> typeWrapStack;
   
   const DeserializerPack(
      super.structHashResolver,
      super.getTagByDartType,
      this.header,
      this.furyDeser,
      this.refResolver,
      this.xtypeResolver,
      this.typeWrapStack,
   );
}