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

import 'dart:collection';
import 'package:fury/src/serializer/collection/collection_serializer_cache.dart';
import 'package:fury/src/serializer/collection/set/set_serializer.dart';
import 'package:fury/src/serializer/serializer.dart';
import 'package:fury/src/serializer/serializer_cache.dart';

final class _SplayTreeSetSerializerCache extends CollectionSerializerCache{
  static SplayTreeSetSerializer? _serRef;
  static SplayTreeSetSerializer? _serNoRef;

  const _SplayTreeSetSerializerCache();

  @override
  Serializer getSerWithRef(bool writeRef){
    if (writeRef){
      _serRef ??= SplayTreeSetSerializer._(true);
      return _serRef!;
    } else {
      _serNoRef ??= SplayTreeSetSerializer._(false);
      return _serNoRef!;
    }
  }
}

final class SplayTreeSetSerializer extends SetSerializer{

  static const SerializerCache cache = _SplayTreeSetSerializerCache();
  static const Object obj = Object();

  SplayTreeSetSerializer._(super.writeRef);

  @override
  Set newSet(bool nullable) {
    return nullable ? SplayTreeSet<Object?>() : SplayTreeSet<Object>();
  }
}