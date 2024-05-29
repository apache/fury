package org.apache.fury.test;

import org.apache.fury.Fury;
import org.apache.fury.TestBase;
import org.apache.spark.sql.types.DecimalType;
import org.apache.spark.sql.types.DecimalType$;
import org.testng.annotations.Test;

public class SparkTypeTest extends TestBase {
  @Test(dataProvider = "enableCodegen")
  public void testObjectType(boolean enableCodegen) {
    Fury fury = builder().withRefTracking(true).withCodegen(enableCodegen).build();
    fury.serialize(DecimalType$.MODULE$);
    fury.serialize(new DecimalType(10, 10));
  }
}
