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

package org.apache.fury.serializer.scala;

import org.apache.fury.Fury;
import org.apache.fury.resolver.ClassResolver;
import org.apache.fury.serializer.Serializer;
import org.apache.fury.serializer.SerializerFactory;

public class ScalaSerializers {

  public static void registerSerializers(Fury fury) {
    ClassResolver resolver = setSerializerFactory(fury);
    resolver.register(scala.collection.immutable.Nil$.class);
    resolver.register(scala.collection.immutable.List$.class);
    resolver.register(scala.collection.immutable.$colon$colon.class);
    resolver.register(scala.collection.immutable.Set.Set1.class);
    resolver.register(scala.collection.immutable.Set.Set2.class);
    resolver.register(scala.collection.immutable.Set.Set3.class);
    resolver.register(scala.collection.immutable.Set.Set4.class);
    resolver.register(scala.collection.immutable.Map$.class);
    resolver.register(scala.collection.immutable.Map.Map1.class);
    resolver.register(scala.collection.immutable.Map.Map2.class);
    resolver.register(scala.collection.immutable.Map.Map3.class);
    resolver.register(scala.collection.immutable.Map.Map4.class);
    resolver.register("scala.collection.immutable.Map$EmptyMap$");
    resolver.register("scala.collection.IterableFactory$ToFactory");
    resolver.register("scala.collection.MapFactory$ToFactory");
    resolver.register(scala.collection.generic.SerializeEnd$.class);
    resolver.register(scala.collection.generic.DefaultSerializationProxy.class);
    resolver.register(scala.runtime.ModuleSerializationProxy.class);
  }

  private static ClassResolver setSerializerFactory(Fury fury) {
    ClassResolver resolver = fury.getClassResolver();
    ScalaDispatcher dispatcher = new ScalaDispatcher();
    SerializerFactory factory = resolver.getSerializerFactory();
    if (factory != null) {
      SerializerFactory newFactory = (f, cls) -> {
        Serializer serializer = factory.createSerializer(f, cls);
        if (serializer == null) {
          serializer = dispatcher.createSerializer(f, cls);
        }
        return serializer;
      };
      resolver.setSerializerFactory(newFactory);
    } else {
      resolver.setSerializerFactory(dispatcher);
    }
    return resolver;
  }
}
