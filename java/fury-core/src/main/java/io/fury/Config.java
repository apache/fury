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

package io.fury;

import io.fury.serializer.CompatibleMode;
import io.fury.serializer.Serializer;
import io.fury.serializer.TimeSerializers;
import io.fury.util.MurmurHash3;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.ObjectOutputStream;
import java.io.Serializable;

/**
 * Fury config.
 *
 * @author chaokunyang
 */
public class Config implements Serializable {
  private final Language language;
  private final boolean referenceTracking;
  private final boolean basicTypesReferenceIgnored;
  private final boolean stringReferenceIgnored;
  private final boolean timeReferenceIgnored;
  private final boolean compressNumber;
  private final boolean compressString;
  private final boolean checkClassVersion;
  private final CompatibleMode compatibleMode;
  private final boolean checkJdkClassSerializable;
  private final Class<? extends Serializer> defaultJDKStreamSerializerType;
  private final boolean secureModeEnabled;
  private final boolean classRegistrationRequired;
  private final boolean metaContextShareEnabled;
  private final boolean codeGenEnabled;
  private final boolean asyncCompilationEnabled;
  private transient int configHash;

  Config(Fury.FuryBuilder builder) {
    language = builder.language;
    referenceTracking = builder.referenceTracking;
    basicTypesReferenceIgnored = !referenceTracking || builder.basicTypesReferenceIgnored;
    stringReferenceIgnored = !referenceTracking || builder.stringReferenceIgnored;
    timeReferenceIgnored = !referenceTracking || builder.timeReferenceIgnored;
    compressNumber = builder.compressNumber;
    compressString = builder.compressString;
    checkClassVersion = builder.checkClassVersion;
    compatibleMode = builder.compatibleMode;
    checkJdkClassSerializable = builder.jdkClassSerializableCheck;
    defaultJDKStreamSerializerType = builder.defaultJDKStreamSerializerType;
    secureModeEnabled = builder.secureModeEnabled;
    classRegistrationRequired = builder.requireClassRegistration;
    metaContextShareEnabled = builder.metaContextShareEnabled;
    codeGenEnabled = builder.codeGenEnabled;
    asyncCompilationEnabled = builder.asyncCompilationEnabled;
  }

  public Language getLanguage() {
    return language;
  }

  public boolean trackingReference() {
    return referenceTracking;
  }

  public boolean isBasicTypesReferenceIgnored() {
    return basicTypesReferenceIgnored;
  }

  public boolean isStringReferenceIgnored() {
    return stringReferenceIgnored;
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
  public boolean isTimeReferenceIgnored() {
    return timeReferenceIgnored;
  }

  public boolean compressNumber() {
    return compressNumber;
  }

  public boolean compressString() {
    return compressString;
  }

  public boolean checkClassVersion() {
    return checkClassVersion;
  }

  public CompatibleMode getCompatibleMode() {
    return compatibleMode;
  }

  /**
   * Returns true if fury need to check whether the class needs to implement {@link Serializable}.
   */
  public boolean checkJdkClassSerializable() {
    return checkJdkClassSerializable;
  }

  /**
   * Returns default serializer type for class which implements jdk serialization method such as
   * `writeObject/readObject`.
   */
  public Class<? extends Serializer> getDefaultJDKStreamSerializerType() {
    return defaultJDKStreamSerializerType;
  }

  public boolean isClassRegistrationRequired() {
    return classRegistrationRequired;
  }

  public boolean isSecureModeEnabled() {
    return secureModeEnabled;
  }

  public boolean isMetaContextShareEnabled() {
    return metaContextShareEnabled;
  }

  /** Whether JIT is enabled. */
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
