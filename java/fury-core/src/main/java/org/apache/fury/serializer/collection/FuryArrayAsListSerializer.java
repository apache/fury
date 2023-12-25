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

package org.apache.fury.serializer.collection;

import org.apache.fury.Fury;
import org.apache.fury.annotation.Internal;
import org.apache.fury.memory.MemoryBuffer;
import org.apache.fury.type.Type;
import java.util.Collection;

/**
 * Serializer for {@link ArrayAsList}. Helper for serialization of other classes.
 *
 * @author chaokunyang
 */
@Internal
@SuppressWarnings("rawtypes")
public final class FuryArrayAsListSerializer extends CollectionSerializer<ArrayAsList> {
  public FuryArrayAsListSerializer(Fury fury) {
    super(fury, ArrayAsList.class, true);
  }

  @Override
  public short getXtypeId() {
    return (short) -Type.LIST.getId();
  }

  public Collection newCollection(MemoryBuffer buffer) {
    numElements = buffer.readPositiveVarInt();
    return new ArrayAsList(numElements);
  }
}
