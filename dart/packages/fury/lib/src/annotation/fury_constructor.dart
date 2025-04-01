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

import 'package:meta/meta_meta.dart';

/// Annotation used to specify which constructor should be used 
/// when Fury deserializes an object.
///
/// Apply this annotation to a constructor to indicate it should be used
/// during the deserialization process by Fury.
///
/// Example:
/// ```dart
/// class Person {
///   late final String name;
///   late final int age;
///
///   // Alternative constructor that won't be used for deserialization
///   Person(this.name, this.age);
///
///   // This constructor will be used by Fury during deserialization
///   @furyConstructor
///   Person.guest();
/// }
/// ```
@Target({TargetKind.constructor})
class FuryConstructor {
  /// The name identifier for this annotation.
  static const String name = 'FuryCons';
  
  /// The valid targets where this annotation can be applied.
  static const List<TargetKind> targets = [TargetKind.constructor];

  /// Creates a new [FuryConstructor] annotation.
  const FuryConstructor();
}

/// A constant instance of [FuryConstructor] for convenient use.
const FuryConstructor furyConstructor = FuryConstructor();
