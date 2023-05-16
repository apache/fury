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

package io.fury.builder;

import io.fury.Fury;
import io.fury.serializer.Serializer;

/**
 * Since janino doesn't support generics, we use {@link Object} to represent object type rather
 * generic type.
 *
 * @author chaokunyang
 */
public interface Generated {
  /** Base class for all generated serializers. */
  abstract class GeneratedSerializer extends Serializer implements Generated {
    public GeneratedSerializer(Fury fury, Class<?> cls) {
      super(fury, cls);
    }
  }

  /** Base class for all type consist serializers. */
  abstract class GeneratedObjectSerializer extends GeneratedSerializer implements Generated {
    public GeneratedObjectSerializer(Fury fury, Class<?> cls) {
      super(fury, cls);
    }
  }
}
