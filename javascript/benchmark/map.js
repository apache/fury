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

const Fury = require("@furyjs/fury");
const hps = require('@furyjs/hps');
const fury = new Fury.default({ hps, refTracking: false, useSliceString: true });
const Benchmark = require("benchmark");
const Type = Fury.Type;



const { serialize, deserialize, serializeVolatile } = fury.registerSerializer(Type.map(Type.any(), Type.any()));
const sample = new Map([["foo", "ba1"],["foo1", "ba1"],["foo2", "ba1"],["foo3", "ba1"],["foo4", "ba1"],["foo5", "ba1"]]);
const furyAb = serialize(sample);


async function start() {

  let result = {
    serialize: 0,
    deserialize: 0
  }

  {
    var suite = new Benchmark.Suite();
    suite
      .add("serialize", function () {
        serializeVolatile(sample).dispose()
      })
      .add("deserialize", function () {
        deserialize(furyAb)
      })
      .on("complete", function (e) {
        e.currentTarget.forEach(({ name, hz }) => {
          result[name] = Math.ceil(hz / 10000);
        });
      })
      .run({ async: false });
  }

  console.table(result);

}
start();
