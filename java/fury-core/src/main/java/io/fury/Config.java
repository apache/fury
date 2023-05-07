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
  private final boolean compressNumber;
  private final boolean secureModeEnabled;
  private final boolean classRegistrationRequired;
  private transient int configHash;

  Config(Fury.FuryBuilder builder) {
    language = builder.language;
    referenceTracking = builder.referenceTracking;
    basicTypesReferenceIgnored = !referenceTracking || builder.basicTypesReferenceIgnored;
    compressNumber = builder.compressNumber;
    secureModeEnabled = builder.secureModeEnabled;
    classRegistrationRequired = builder.requireClassRegistration;
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

  public boolean compressNumber() {
    return compressNumber;
  }

  public boolean isClassRegistrationRequired() {
    return classRegistrationRequired;
  }

  public boolean isSecureModeEnabled() {
    return secureModeEnabled;
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
