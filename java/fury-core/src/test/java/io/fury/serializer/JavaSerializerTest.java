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
import io.fury.FuryTestBase;
import io.fury.Language;
import java.io.IOException;
import java.io.Serializable;
import lombok.Data;
import org.testng.annotations.Test;

public class JavaSerializerTest extends FuryTestBase {

  @Data
  public static class CustomClass implements Serializable {
    public String name;
    public transient int age;

    private void writeObject(java.io.ObjectOutputStream s) throws IOException {
      s.defaultWriteObject();
      s.writeInt(age);
    }

    private void readObject(java.io.ObjectInputStream s) throws Exception {
      s.defaultReadObject();
      this.age = s.readInt();
    }
  }

  @Test
  public void testWriteObject() {
    Fury fury =
        Fury.builder()
            .withLanguage(Language.JAVA)
            .withRefTracking(false)
            .disableSecureMode()
            .build();
    serDe(fury, new CustomClass());
  }
}
