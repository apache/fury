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

package io.fury.resolver;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertTrue;

import io.fury.memory.MemoryBuffer;
import java.util.Map;
import org.testng.annotations.Test;

public class MapReferenceResolverTest {

  @Test
  public void testTrackingReference() {
    MapReferenceResolver referenceResolver = new MapReferenceResolver();
    MemoryBuffer buffer = MemoryBuffer.newHeapBuffer(32);
    assertTrue(referenceResolver.writeReferenceOrNull(buffer, null));
    assertFalse(referenceResolver.writeReferenceOrNull(buffer, new Object()));
    Object o = new Object();
    assertFalse(referenceResolver.writeReferenceOrNull(buffer, o));
    assertTrue(referenceResolver.writeReferenceOrNull(buffer, o));
    assertFalse(referenceResolver.writeNullFlag(buffer, o));
    assertTrue(referenceResolver.writeNullFlag(buffer, null));
  }

  @Test
  public void testReferenceStatistics() {
    // MapReferenceResolver may be loaded and run already, set flag won't take effect.
    // If java jit run and optimized `writeReferenceOrNull`, set `ENABLE_FURY_REF_PROFILING`
    // by reflection may not take effect too.
    // System.setProperty("fury.enable_ref_profiling", "true");
    MapReferenceResolver referenceResolver = new MapReferenceResolver();
    MemoryBuffer buffer = MemoryBuffer.newHeapBuffer(32);
    Object obj1 = new Object();
    referenceResolver.writeReferenceOrNull(buffer, obj1);
    referenceResolver.writeReferenceOrNull(buffer, obj1);
    referenceResolver.writeReferenceOrNull(buffer, "abc");
    referenceResolver.writeReferenceOrNull(buffer, "abcd");
    referenceResolver.writeReferenceOrNull(buffer, String.class);
    MapReferenceResolver.ReferenceStatistics referenceStatistics =
        referenceResolver.referenceStatistics();
    Map<Class<?>, Integer> summary = referenceStatistics.referenceTypeSummary;
    // check order, type most frequent occurs first.
    assertEquals(summary.entrySet().stream().iterator().next().getKey(), String.class);
    assertEquals(summary.get(Object.class).intValue(), 1);
    assertEquals(summary.get(String.class).intValue(), 2);
    assertEquals(summary.get(Class.class).intValue(), 1);
    // assertTrue(referenceStatistics.mapStatistics.totalProbeProfiled > 0);
    // assertTrue(referenceStatistics.mapStatistics.maxProbeProfiled > 0);
    // assertTrue(referenceStatistics.referenceCount > 0);
  }
}
