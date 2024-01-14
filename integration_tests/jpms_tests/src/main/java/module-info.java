module org.apache.fury.integration_tests {

    requires org.apache.fury.benchmark;
    requires org.apache.fury.core;
    requires org.apache.fury.format;
    requires org.apache.fury.test.core;

    // we can't really test any classes from this module because it only contains test-classes
    requires org.apache.fury.test.suite;
}