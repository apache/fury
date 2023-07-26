package io.fury.integration_tests.state;

import io.fury.Fury;
import io.fury.benchmark.data.MediaContent;
import io.fury.benchmark.data.Sample;
import org.testng.Assert;
import org.testng.annotations.Test;

public class FuryTest {

  @Test
  public void testMediaContent() {
    Sample object = new Sample().populate(false);
    Fury fury = Fury.builder().withSecureMode(false).build();
    byte[] data = fury.serialize(object);
    Sample sample = (Sample) fury.deserialize(data);
    Assert.assertEquals(sample, object);
  }

  @Test
  public void testSample() {
    MediaContent object = new MediaContent().populate(false);
    Fury fury = Fury.builder().withSecureMode(false).build();
    byte[] data = fury.serialize(object);
    MediaContent mediaContent = (MediaContent) fury.deserialize(data);
    Assert.assertEquals(mediaContent, object);
  }
}
