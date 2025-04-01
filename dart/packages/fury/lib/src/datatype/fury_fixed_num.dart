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

import 'float16.dart';
import 'float32.dart';
import 'int16.dart';
import 'int32.dart';
import 'int8.dart';

/// Base abstract class for fixed-size numeric types
abstract base class FixedNum implements Comparable<FixedNum>{

  num get value;

  // Factory constructor to create the appropriate type
  static FixedNum from(num value, {String type = 'int32'}) {
    switch (type.toLowerCase()) {
      case 'int8': return Int8(value);
      case 'int16': return Int16(value);
      case 'int32': return Int32(value);
      case 'float16': return Float16(value);
      case 'float32': return Float32(value);
      default: throw ArgumentError('Unknown fixed numeric type: $type');
    }
  }

  @override
  int compareTo(FixedNum other) => value.compareTo(other.value);
}