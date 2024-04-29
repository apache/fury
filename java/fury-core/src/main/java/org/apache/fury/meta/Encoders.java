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

package org.apache.fury.meta;

import static org.apache.fury.meta.MetaString.Encoding.*;

import java.util.Arrays;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import org.apache.fury.meta.MetaString.Encoding;

public class Encoders {
  public static final MetaStringEncoder PACKAGE_ENCODER = new MetaStringEncoder('.', '_');
  public static final MetaStringDecoder PACKAGE_DECODER = new MetaStringDecoder('.', '_');
  public static final MetaStringEncoder TYPE_NAME_ENCODER = new MetaStringEncoder('$', '_');
  public static final MetaStringDecoder TYPE_NAME_DECODER = new MetaStringDecoder('$', '_');
  static final MetaStringEncoder FIELD_NAME_ENCODER = new MetaStringEncoder('$', '_');
  static final MetaStringDecoder FIELD_NAME_DECODER = new MetaStringDecoder('$', '_');
  private static final ConcurrentMap<String, MetaString> pgkMetaStringCache =
      new ConcurrentHashMap<>();
  private static final ConcurrentMap<String, MetaString> typeMetaStringCache =
      new ConcurrentHashMap<>();
  private static final ConcurrentMap<String, MetaString> fieldMetaStringCache =
      new ConcurrentHashMap<>();
  static final Encoding[] pkgEncodings =
      new Encoding[] {UTF_8, ALL_TO_LOWER_SPECIAL, LOWER_UPPER_DIGIT_SPECIAL};
  static final List<Encoding> pkgEncodingsList = Arrays.asList(pkgEncodings);

  static final Encoding[] typeNameEncodings =
      new Encoding[] {
        UTF_8, LOWER_UPPER_DIGIT_SPECIAL, FIRST_TO_LOWER_SPECIAL, ALL_TO_LOWER_SPECIAL
      };
  static final List<Encoding> typeNameEncodingsList = Arrays.asList(typeNameEncodings);

  static final Encoding[] fieldNameEncodings =
      new Encoding[] {UTF_8, LOWER_UPPER_DIGIT_SPECIAL, ALL_TO_LOWER_SPECIAL};
  static final List<Encoding> fieldNameEncodingsList = Arrays.asList(fieldNameEncodings);

  public static MetaString encodePackage(String pkg) {
    return pgkMetaStringCache.computeIfAbsent(pkg, k -> PACKAGE_ENCODER.encode(pkg, pkgEncodings));
  }

  public static MetaString encodeTypeName(String typeName) {
    return typeMetaStringCache.computeIfAbsent(
        typeName, k -> TYPE_NAME_ENCODER.encode(typeName, typeNameEncodings));
  }

  public static MetaString encodeFieldName(String fieldName) {
    return fieldMetaStringCache.computeIfAbsent(
        fieldName, k -> FIELD_NAME_ENCODER.encode(fieldName, fieldNameEncodings));
  }
}
