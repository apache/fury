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

package org.apache.fury.integration_tests;

import org.apache.fury.Fury;
import org.apache.fury.benchmark.Benchmark;
import org.apache.fury.format.encoder.Encoders;
import org.apache.fury.test.bean.Foo;

/**
 * A test class that simply references classes from the various Fury artifacts to check whether
 * they specify the module names referenced in the module-info descriptor.
 */
public class Test {

    public static void main(String[] args) {
        final Fury fury = Fury.builder().build();
        fury.serialize(Foo.create());

        Encoders.bean(Benchmark.class, fury);
    }
}
