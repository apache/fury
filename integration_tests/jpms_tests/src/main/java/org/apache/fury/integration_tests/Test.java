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
