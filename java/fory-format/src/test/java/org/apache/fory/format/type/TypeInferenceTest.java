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

import com.google.common.base.CaseFormat;
import java.util.List;
import java.util.stream.Collectors;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.Schema;
import org.apache.fory.test.bean.BeanA;
import org.apache.fory.type.Descriptor;
import org.testng.annotations.Test;

public class TypeInferenceTest {
  @Test
  public void inferDataType() {
    List<Descriptor> descriptors = Descriptor.getDescriptors(BeanA.class);
    Schema schema = TypeInference.inferSchema(BeanA.class);
    List<String> fieldNames =
        schema.getFields().stream().map(Field::getName).collect(Collectors.toList());
    List<String> expectedFieldNames =
        descriptors.stream()
            .map(d -> CaseFormat.LOWER_CAMEL.to(CaseFormat.LOWER_UNDERSCORE, d.getName()))
            .collect(Collectors.toList());
    assertEquals(fieldNames, expectedFieldNames);
    schema.findField("double_list");
  }
}
