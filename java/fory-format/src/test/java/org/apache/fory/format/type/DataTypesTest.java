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

package org.apache.fory.format.type;

import static org.testng.Assert.assertEquals;

import java.nio.ByteBuffer;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.Schema;
import org.apache.fory.test.bean.BeanA;
import org.testng.annotations.Test;

public class DataTypesTest {

  @Test
  public void parseSchema() {
    Schema schema = TypeInference.inferSchema(BeanA.class);
    byte[] bytes = schema.toByteArray();
    Schema parsedSchema = Schema.deserialize(ByteBuffer.wrap(bytes));
    assertEquals(schema, parsedSchema);
  }

  @Test
  public void testDataTypes() {
    assertEquals(DataTypes.intType(8), DataTypes.int8());
    assertEquals(DataTypes.intType(16), DataTypes.int16());
    assertEquals(DataTypes.intType(32), DataTypes.int32());
    assertEquals(DataTypes.intType(64), DataTypes.int64());
    assertEquals(DataTypes.utf8(), ArrowType.Utf8.INSTANCE);
  }
}
