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

import static org.testng.Assert.assertNotNull;

import org.apache.fury.Fury;
import org.apache.fury.config.Language;
import org.testng.annotations.Test;

public class ClassInfoTest {
  @Test
  public void testEncodePackageNameAndTypeName() {
    Fury fury1 = Fury.builder().withLanguage(Language.JAVA).requireClassRegistration(false).build();
    ClassInfo info1 = fury1.getClassResolver().getClassInfo(org.apache.fury.test.bean.Foo.class);
    assertNotNull(info1.namespaceBytes);
    assertNotNull(info1.typeNameBytes);
  }
}
