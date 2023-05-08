/*
 * Copyright 2023 The Fury authors
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package io.fury.serializer;

import io.fury.Fury;
import io.fury.collection.Tuple3;
import io.fury.memory.MemoryBuffer;
import java.util.HashMap;
import java.util.Locale;
import java.util.Map;

/**
 * Local serializer for {@link Locale}.
 *
 * @author chaokunyang
 */
public final class LocaleSerializer extends Serializer<Locale> {
  private static final Map<Tuple3<String, String, String>, Object> LOCALE_CACHE = new HashMap<>();

  static {
    putLocalCache(Locale.US);
    putLocalCache(Locale.SIMPLIFIED_CHINESE);
    putLocalCache(Locale.CHINESE);
    putLocalCache(Locale.TRADITIONAL_CHINESE);
    putLocalCache(Locale.ENGLISH);
    putLocalCache(Locale.GERMAN);
    putLocalCache(Locale.FRENCH);
    putLocalCache(Locale.ITALIAN);
    putLocalCache(Locale.JAPANESE);
    putLocalCache(Locale.KOREAN);
    putLocalCache(Locale.CHINA);
    putLocalCache(Locale.TAIWAN);
    putLocalCache(Locale.UK);
    putLocalCache(Locale.GERMANY);
    putLocalCache(Locale.FRANCE);
    putLocalCache(Locale.ITALY);
    putLocalCache(Locale.JAPAN);
    putLocalCache(Locale.KOREA);
    putLocalCache(Locale.PRC);
    putLocalCache(Locale.CANADA);
    putLocalCache(Locale.CANADA_FRENCH);
    putLocalCache(Locale.ROOT);
    putLocalCache(new Locale("es", "", ""));
    putLocalCache(new Locale("es", "ES", ""));
  }

  private static void putLocalCache(Locale locale) {
    LOCALE_CACHE.put(
        Tuple3.of(locale.getCountry(), locale.getLanguage(), locale.getVariant()), locale);
  }

  public LocaleSerializer(Fury fury) {
    super(fury, Locale.class);
  }

  public void write(MemoryBuffer buffer, Locale l) {
    fury.writeJavaString(buffer, l.getLanguage());
    fury.writeJavaString(buffer, l.getCountry());
    fury.writeJavaString(buffer, l.getVariant());
  }

  public Locale read(MemoryBuffer buffer) {
    String language = fury.readJavaString(buffer);
    String country = fury.readJavaString(buffer);
    String variant = fury.readJavaString(buffer);
    // Fast path for Default/US/SIMPLIFIED_CHINESE
    Locale defaultLocale = Locale.getDefault();
    if (isSame(defaultLocale, language, country, variant)) {
      return defaultLocale;
    }
    if (defaultLocale != Locale.US && isSame(Locale.US, language, country, variant)) {
      return Locale.US;
    }
    if (isSame(Locale.SIMPLIFIED_CHINESE, language, country, variant)) {
      return Locale.SIMPLIFIED_CHINESE;
    }
    Object o = LOCALE_CACHE.get(Tuple3.of(language, country, variant));
    if (o != null) {
      return (Locale) o;
    }
    return new Locale(language, country, variant);
  }

  static boolean isSame(Locale locale, String language, String country, String variant) {
    return (locale.getLanguage().equals(language)
        && locale.getCountry().equals(country)
        && locale.getVariant().equals(variant));
  }
}
