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

import 'package:fury/src/codegen/entity/struct_hash_pair.dart';
import 'package:fury/src/memory/byte_reader.dart';
import 'package:fury/src/meta/type_info.dart';
import 'package:fury/src/meta/spec_wraps/type_spec_wrap.dart';
import 'package:fury/src/resolver/impl/xtype_resolver_impl.dart';
import 'package:fury/src/serializer/serializer.dart';
import 'package:fury/src/config/fury_config.dart';
import 'package:fury/src/memory/byte_writer.dart';
import 'package:fury/src/meta/specs/custom_type_spec.dart';
import 'package:fury/src/serializer_pack.dart';

abstract base class XtypeResolver{

  const XtypeResolver(FuryConfig conf);

  static XtypeResolver newOne(FuryConfig conf) {
    return XtypeResolverImpl(conf);
  }

  void reg(CustomTypeSpec spec, [String? tag]);

  void registerSerializer(Type type, Serializer ser);

  void setSersForTypeWrap(List<TypeSpecWrap> typeWraps);

  TypeInfo readTypeInfo(ByteReader br);

  String getTagByCustomDartType(Type type);

  TypeInfo writeGetTypeInfo(ByteWriter bw, Object obj, SerializerPack pack);

  /*-----For test only------------------------------------------------*/
  StructHashPair getHashPairForTest(
    Type type,
  );
}