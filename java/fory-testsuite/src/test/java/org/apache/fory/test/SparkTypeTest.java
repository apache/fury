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

package org.apache.fory.test;

import org.apache.fory.Fory;
import org.apache.fory.TestBase;
import org.apache.spark.sql.types.DecimalType;
import org.apache.spark.sql.types.DecimalType$;
import org.testng.annotations.Test;

public class SparkTypeTest extends TestBase {
  @Test(dataProvider = "enableCodegen")
  public void testObjectType(boolean enableCodegen) {
    Fory fory = builder().withRefTracking(true).withCodegen(enableCodegen).build();
    fory.serialize(DecimalType$.MODULE$);
    fory.serialize(new DecimalType(10, 10));
  }
}
