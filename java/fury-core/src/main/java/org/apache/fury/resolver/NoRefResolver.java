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

package org.apache.fury.resolver;

import org.apache.fury.Fury;
import org.apache.fury.memory.MemoryBuffer;

/** A no-op resolver which ignore reference, only handle null/non-null. */
public final class NoRefResolver implements RefResolver {

  @Override
  public boolean writeRefOrNull(MemoryBuffer buffer, Object obj) {
    if (obj == null) {
      buffer.writeByte(Fury.NULL_FLAG);
      return true;
    } else {
      buffer.writeByte(Fury.NOT_NULL_VALUE_FLAG);
      return false;
    }
  }

  @Override
  public boolean writeRefValueFlag(MemoryBuffer buffer, Object obj) {
    assert obj != null;
    buffer.writeByte(Fury.NOT_NULL_VALUE_FLAG);
    return true;
  }

  @Override
  public boolean writeNullFlag(MemoryBuffer buffer, Object obj) {
    if (obj == null) {
      buffer.writeByte(Fury.NULL_FLAG);
      return true;
    }
    return false;
  }

  @Override
  public void replaceRef(Object original, Object newObject) {}

  @Override
  public byte readRefOrNull(MemoryBuffer buffer) {
    return buffer.readByte();
  }

  @Override
  public int preserveRefId() {
    return -1;
  }

    @Override
    public int preserveRefId(int refId) {
        return -1;
    }

    @Override
  public int tryPreserveRefId(MemoryBuffer buffer) {
    // `NOT_NULL_VALUE_FLAG` can be used as stub reference id because we use
    // `refId >= NOT_NULL_VALUE_FLAG` to read data.
    return buffer.readByte();
  }

  @Override
  public int lastPreservedRefId() {
    return -1;
  }

  @Override
  public void reference(Object object) {}

  @Override
  public Object getReadObject(int id) {
    return null;
  }

  @Override
  public Object getReadObject() {
    return null;
  }

  @Override
  public void setReadObject(int id, Object object) {}

  @Override
  public void reset() {}

  @Override
  public void resetWrite() {}

  @Override
  public void resetRead() {}
}
