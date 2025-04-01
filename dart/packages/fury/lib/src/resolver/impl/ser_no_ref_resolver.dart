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

import 'package:fury/src/const/ref_flag.dart';
import 'package:fury/src/dev_annotation/optimize.dart';
import 'package:fury/src/resolver/serialization_ref_resolver.dart';

final class SerNoRefResolver extends SerializationRefResolver{
  static const SerNoRefResolver _instance = SerNoRefResolver._();
  factory SerNoRefResolver() => _instance;
  // private constructor
  const SerNoRefResolver._();

  static final SerializationRefMeta noRef = (refFlag: RefFlag.NULL, refId: null);
  static final SerializationRefMeta untrackNotNull = (refFlag: RefFlag.UNTRACK_NOT_NULL, refId: null);

  @inline
  @override
  SerializationRefMeta getRefId(Object? obj) {
    return obj == null ? noRef : untrackNotNull;
  }

  @override
  @inline
  RefFlag getRefFlag(Object? obj) {
    return obj == null ? RefFlag.NULL : RefFlag.UNTRACK_NOT_NULL;
  }
}