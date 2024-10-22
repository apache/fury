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

import org.apache.fury.AbstractThreadSafeFury;
import org.apache.fury.Fury;
import org.apache.fury.ThreadSafeFury;
import org.apache.fury.resolver.ClassResolver;
import org.apache.fury.serializer.Serializer;
import org.apache.fury.serializer.SerializerFactory;
import scala.collection.immutable.NumericRange;
import scala.collection.immutable.Range;

import static org.apache.fury.serializer.scala.ToFactorySerializers.IterableToFactoryClass;
import static org.apache.fury.serializer.scala.ToFactorySerializers.MapToFactoryClass;

public class ScalaSerializers {

  public static void registerSerializers(ThreadSafeFury fury) {
    AbstractThreadSafeFury threadSafeFury = (AbstractThreadSafeFury) fury;
    threadSafeFury.registerCallback(ScalaSerializers::registerSerializers);
  }

  public static void registerSerializers(Fury fury) {
    ClassResolver resolver = setSerializerFactory(fury);

    resolver.registerSerializer(IterableToFactoryClass, new ToFactorySerializers.IterableToFactorySerializer(fury));
    resolver.registerSerializer(MapToFactoryClass, new ToFactorySerializers.MapToFactorySerializer(fury));

    // Seq
    resolver.register(scala.collection.immutable.Seq.class);
    resolver.register(scala.collection.immutable.Nil$.class);
    resolver.register(scala.collection.immutable.List$.class);
    resolver.register(scala.collection.immutable.$colon$colon.class);
    // StrictOptimizedSeqFactory -> ... extends -> IterableFactory
    resolver.register(scala.collection.immutable.Vector$.class);
    resolver.register("scala.collection.immutable.VectorImpl");
    resolver.register("scala.collection.immutable.Vector0");
    resolver.register("scala.collection.immutable.Vector1");
    resolver.register("scala.collection.immutable.Vector2");
    resolver.register("scala.collection.immutable.Vector3");
    resolver.register("scala.collection.immutable.Vector4");
    resolver.register("scala.collection.immutable.Vector5");
    resolver.register("scala.collection.immutable.Vector6");
    resolver.register(scala.collection.immutable.Queue.class);
    resolver.register(scala.collection.immutable.Queue$.class);
    resolver.register(scala.collection.immutable.LazyList.class);
    resolver.register(scala.collection.immutable.LazyList$.class);
    resolver.register(scala.collection.immutable.ArraySeq.class);
    resolver.register(scala.collection.immutable.ArraySeq$.class);

    // Set
    resolver.register(scala.collection.immutable.Set.class);
    // IterableFactory
    resolver.register(scala.collection.immutable.Set$.class);
    resolver.register(scala.collection.immutable.Set.Set1.class);
    resolver.register(scala.collection.immutable.Set.Set2.class);
    resolver.register(scala.collection.immutable.Set.Set3.class);
    resolver.register(scala.collection.immutable.Set.Set4.class);
    resolver.register(scala.collection.immutable.HashSet.class);
    resolver.register(scala.collection.immutable.TreeSet.class);
    // SortedIterableFactory
    resolver.register(scala.collection.immutable.TreeSet$.class);
    // IterableFactory
    resolver.register(scala.collection.immutable.HashSet$.class);
    resolver.register(scala.collection.immutable.ListSet.class);
    resolver.register(scala.collection.immutable.ListSet$.class);
    resolver.register("scala.collection.immutable.Set$EmptySet$");
    resolver.register("scala.collection.immutable.SetBuilderImpl");
    resolver.register("scala.collection.immutable.SortedMapOps$ImmutableKeySortedSet");

    // Map
    resolver.register(scala.collection.immutable.Map.class);
    resolver.register(scala.collection.immutable.Map$.class);
    resolver.register(scala.collection.immutable.Map.Map1.class);
    resolver.register(scala.collection.immutable.Map.Map2.class);
    resolver.register(scala.collection.immutable.Map.Map3.class);
    resolver.register(scala.collection.immutable.Map.Map4.class);
    resolver.register(scala.collection.immutable.Map.WithDefault.class);
    resolver.register("scala.collection.immutable.MapBuilderImpl");
    resolver.register("scala.collection.immutable.Map$EmptyMap$");
    resolver.register("scala.collection.immutable.SeqMap$EmptySeqMap$");
    resolver.register(scala.collection.immutable.HashMap.class);
    resolver.register(scala.collection.immutable.HashMap$.class);
    resolver.register(scala.collection.immutable.TreeMap.class);
    resolver.register(scala.collection.immutable.TreeMap$.class);
    resolver.register(scala.collection.immutable.SortedMap$.class);
    resolver.register(scala.collection.immutable.TreeSeqMap.class);
    resolver.register(scala.collection.immutable.TreeSeqMap$.class);
    resolver.register(scala.collection.immutable.ListMap.class);
    resolver.register(scala.collection.immutable.ListMap$.class);
    resolver.register(scala.collection.immutable.IntMap.class);
    resolver.register(scala.collection.immutable.IntMap$.class);
    resolver.register(scala.collection.immutable.LongMap.class);
    resolver.register(scala.collection.immutable.LongMap$.class);

    // Range
    resolver.register("scala.math.Numeric$IntIsIntegral$");
    resolver.register("scala.math.Numeric$LongIsIntegral$");
    resolver.registerSerializer(Range.Inclusive.class, new RangeSerializer(fury, Range.Inclusive.class));
    resolver.registerSerializer(Range.Exclusive.class, new RangeSerializer(fury, Range.Exclusive.class));
    resolver.registerSerializer(NumericRange.class, new NumericRangeSerializer<>(fury, NumericRange.class));
    resolver.registerSerializer(NumericRange.Exclusive.class,
      new NumericRangeSerializer<>(fury, NumericRange.Exclusive.class));
    resolver.registerSerializer(NumericRange.Inclusive.class,
      new NumericRangeSerializer<>(fury, NumericRange.Inclusive.class));

    resolver.register(scala.collection.generic.SerializeEnd$.class);
    resolver.register(scala.collection.generic.DefaultSerializationProxy.class);
    resolver.register(scala.runtime.ModuleSerializationProxy.class);

    // mutable collection types
    resolver.register(scala.collection.mutable.StringBuilder.class);
    resolver.register(scala.collection.mutable.ArrayBuffer.class);
    resolver.register(scala.collection.mutable.ArrayBuffer$.class);
    resolver.register(scala.collection.mutable.ArraySeq.class);
    resolver.register(scala.collection.mutable.ArraySeq$.class);
    resolver.register(scala.collection.mutable.ListBuffer.class);
    resolver.register(scala.collection.mutable.ListBuffer$.class);
    resolver.register(scala.collection.mutable.Buffer$.class);
    resolver.register(scala.collection.mutable.ArrayDeque.class);
    resolver.register(scala.collection.mutable.ArrayDeque$.class);

    resolver.register(scala.collection.mutable.HashSet.class);
    resolver.register(scala.collection.mutable.HashSet$.class);
    resolver.register(scala.collection.mutable.TreeSet.class);
    resolver.register(scala.collection.mutable.TreeSet$.class);

    resolver.register(scala.collection.mutable.HashMap.class);
    resolver.register(scala.collection.mutable.HashMap$.class);
    resolver.register(scala.collection.mutable.TreeMap.class);
    resolver.register(scala.collection.mutable.TreeMap$.class);
    resolver.register(scala.collection.mutable.LinkedHashMap.class);
    resolver.register(scala.collection.mutable.LinkedHashMap$.class);
    resolver.register(scala.collection.mutable.LinkedHashSet.class);
    resolver.register(scala.collection.mutable.LinkedHashSet$.class);
    resolver.register(scala.collection.mutable.LongMap.class);
    resolver.register(scala.collection.mutable.LongMap$.class);

    resolver.register(scala.collection.mutable.Queue.class);
    resolver.register(scala.collection.mutable.Queue$.class);
    resolver.register(scala.collection.mutable.Stack.class);
    resolver.register(scala.collection.mutable.Stack$.class);
    resolver.register(scala.collection.mutable.BitSet.class);
    resolver.register(scala.collection.mutable.BitSet$.class);
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
