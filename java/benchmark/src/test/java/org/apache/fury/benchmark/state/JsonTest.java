/*
 * Copyright (C) 2007 The Guava Authors
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except
 * in compliance with the License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License
 * is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express
 * or implied. See the License for the specific language governing permissions and limitations under
 * the License.
 */

package org.apache.fury.benchmark.state;

import com.alibaba.fastjson2.JSONObject;
import org.apache.fury.Fury;
import org.apache.fury.config.CompatibleMode;
import org.apache.fury.config.Language;
import org.testng.Assert;
import org.testng.annotations.Test;

public class JsonTest {
  public static class DemoResponse {
    private JSONObject json;

    public DemoResponse(JSONObject json) {
      this.json = json;
    }
  }

  @Test
  public void testSerializeJson() {
    // For issue: https://github.com/apache/incubator-fury/issues/1604
    JSONObject jsonObject = new JSONObject();
    jsonObject.put("k1", "v1");
    jsonObject.put("k2", "v2");
    DemoResponse resp = new DemoResponse(jsonObject);
    Fury fury =
        Fury.builder()
            .withLanguage(Language.JAVA)
            .requireClassRegistration(false)
            .withRefTracking(true)
            .registerGuavaTypes(false)
            .withCompatibleMode(CompatibleMode.COMPATIBLE)
            .build();
    byte[] serialized = fury.serialize(resp);
    DemoResponse o = (DemoResponse) fury.deserialize(serialized);
    Assert.assertEquals(o.json, jsonObject);
  }
}
