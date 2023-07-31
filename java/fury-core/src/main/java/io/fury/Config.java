/*
 * Copyright 2023 The Fury Authors
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package io.fury;

import com.google.common.base.Preconditions;
import io.fury.serializer.CompatibleMode;
import io.fury.serializer.Serializer;
import io.fury.serializer.TimeSerializers;
import io.fury.util.MurmurHash3;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.ObjectOutputStream;
import java.io.Serializable;

@SuppressWarnings({"rawtypes"})
public class Config implements Serializable {
  private final Language language;
  private final boolean trackingRef;
  private final boolean basicTypesRefIgnored;
  private final boolean stringRefIgnored;
  private final boolean timeRefIgnored;
  private final boolean codeGenEnabled;
  private final boolean checkClassVersion;
  private final CompatibleMode compatibleMode;
  private final boolean checkJdkClassSerializable;
  private final Class<? extends Serializer> defaultJDKStreamSerializerType;
  private final boolean compressString;
  private final boolean compressNumber;
  private final boolean requireClassRegistration;
  private final boolean registerGuavaTypes;
  private final boolean shareMetaContext;
  private final boolean asyncCompilationEnabled;
  private final boolean deserializeUnexistedClass;
  private transient int configHash;

  Config(Fury.FuryBuilder builder) {
    language = builder.language;
    trackingRef = builder.trackingRef;
    basicTypesRefIgnored = !trackingRef || builder.basicTypesRefIgnored;
    stringRefIgnored = !trackingRef || builder.stringRefIgnored;
    timeRefIgnored = !trackingRef || builder.timeRefIgnored;
    compressString = builder.compressString;
    compressNumber = builder.compressNumber;
    requireClassRegistration = builder.requireClassRegistration;
    registerGuavaTypes = builder.registerGuavaTypes;
    codeGenEnabled = builder.codeGenEnabled;
    checkClassVersion = builder.checkClassVersion;
    compatibleMode = builder.compatibleMode;
    checkJdkClassSerializable = builder.checkJdkClassSerializable;
    defaultJDKStreamSerializerType = builder.defaultJDKStreamSerializerType;
    shareMetaContext = builder.shareMetaContext;
    deserializeUnexistedClass = builder.deserializeUnexistedClass;
    if (deserializeUnexistedClass) {
      // Only in meta share mode or compatibleMode, fury knows how to deserialize
      // exist class by type info in data.
      Preconditions.checkArgument(shareMetaContext || compatibleMode == CompatibleMode.COMPATIBLE);
    }
    asyncCompilationEnabled = builder.asyncCompilationEnabled;
  }

  public Language getLanguage() {
    return language;
  }

  public boolean trackingRef() {
    return trackingRef;
  }

  public boolean isBasicTypesRefIgnored() {
    return basicTypesRefIgnored;
  }

  public boolean isStringRefIgnored() {
    return stringRefIgnored;
  }

  /**
   * Whether ignore reference tracking of all time types registered in {@link TimeSerializers} and
   * subclasses of those types when ref tracking is enabled.
   *
   * <p>If ignored, ref tracking of every time type can be enabled by invoke {@link
   * Fury#registerSerializer(Class, Serializer)}, ex:
   *
   * <pre>
   *   fury.registerSerializer(Date.class, new DateSerializer(fury, true));
   * </pre>
   *
   * <p>Note that enabling ref tracking should happen before serializer codegen of any types which
   * contains time fields. Otherwise, those fields will still skip ref tracking.
   */
  public boolean isTimeRefIgnored() {
    return timeRefIgnored;
  }

  public boolean checkClassVersion() {
    return checkClassVersion;
  }

  public CompatibleMode getCompatibleMode() {
    return compatibleMode;
  }

  public boolean checkJdkClassSerializable() {
    return checkJdkClassSerializable;
  }

  public boolean compressString() {
    return compressString;
  }

  public boolean compressNumber() {
    return compressNumber;
  }

  public boolean requireClassRegistration() {
    return requireClassRegistration;
  }

  public boolean registerGuavaTypes() {
    return registerGuavaTypes;
  }

  /**
   * Returns default serializer type for class which implements jdk serialization method such as
   * `writeObject/readObject`.
   */
  public Class<? extends Serializer> getDefaultJDKStreamSerializerType() {
    return defaultJDKStreamSerializerType;
  }

  public boolean shareMetaContext() {
    return shareMetaContext;
  }

  /**
   * Whether deserialize/skip data of un-existed class. If not enabled, an exception will be thrown
   * if class not exist.
   */
  public boolean deserializeUnexistedClass() {
    return deserializeUnexistedClass;
  }

  /**
   * Whether JIT is enabled.
   *
   * @see #isAsyncCompilationEnabled
   */
  public boolean isCodeGenEnabled() {
    return codeGenEnabled;
  }

  /**
   * Whether enable async compilation. If enabled, serialization will use interpreter mode
   * serialization first and switch to jit serialization after async serializer jit for a class \ is
   * finished.
   */
  public boolean isAsyncCompilationEnabled() {
    return asyncCompilationEnabled;
  }

  public int getConfigHash() {
    if (configHash == 0) {
      // TODO use a custom encoding to ensure different config hash different hash.
      ByteArrayOutputStream bas = new ByteArrayOutputStream();
      try (ObjectOutputStream stream = new ObjectOutputStream(bas)) {
        stream.writeObject(this);
        stream.flush();
      } catch (IOException e) {
        throw new RuntimeException(e);
      }
      byte[] bytes = bas.toByteArray();
      long hashPart1 = MurmurHash3.murmurhash3_x64_128(bytes, 0, bytes.length, 47)[0];
      configHash = Math.abs((int) hashPart1);
    }
    return configHash;
  }
}
