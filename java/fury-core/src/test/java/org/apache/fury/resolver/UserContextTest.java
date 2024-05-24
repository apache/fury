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
import org.apache.fury.FuryTestBase;
import org.apache.fury.config.CompatibleMode;
import org.apache.fury.config.Language;
import org.apache.fury.memory.MemoryBuffer;
import org.apache.fury.test.bean.Foo;
import org.testng.Assert;
import org.testng.annotations.Test;

public class UserContextTest extends FuryTestBase {

  public class DataUserContext extends UserContext {
    Object data = null;

    public DataUserContext(Fury fury) {
      super(fury);
    }

    @Override
    public void write(MemoryBuffer buffer) {
      fury.writeRef(buffer, data);
    }

    @Override
    public void read(MemoryBuffer buffer) {
      data = fury.readRef(buffer);
    }
  }

  @Test
  public void checkShareUserContext() {
    Fury fury =
        Fury.builder()
            .withLanguage(Language.JAVA)
            .requireClassRegistration(false)
            .withCompatibleMode(CompatibleMode.COMPATIBLE)
            .withMetaContextShare(true)
            .withUserContextShare(true)
            .build();
    final Foo o = Foo.create();
    final SerializationContext serializationContext = fury.getSerializationContext();
    serializationContext.setMetaContext(new MetaContext());
    final DataUserContext userContext1 = new DataUserContext(fury);
    userContext1.data = "test1";
    serializationContext.addUserContext(userContext1);
    final byte[] bytes = fury.serialize(o);

    serializationContext.setMetaContext(new MetaContext());
    final DataUserContext userContext2 = new DataUserContext(fury);
    serializationContext.addUserContext(userContext2);

    Assert.assertEquals(fury.deserialize(bytes), o);
    Assert.assertEquals(userContext1.data, userContext2.data);
  }
}
