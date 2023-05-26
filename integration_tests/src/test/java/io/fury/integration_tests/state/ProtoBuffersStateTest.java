package io.fury.integration_tests.state;

import io.fury.benchmark.data.MediaContent;
import io.fury.benchmark.data.Sample;
import org.testng.Assert;
import org.testng.annotations.Test;

public class ProtoBuffersStateTest {
  @Test
  public void testMediaContent() {
    Sample object = new Sample().populate(false);
    byte[] data = ProtoBuffersState.serializeSample(object);
    Sample sample = ProtoBuffersState.deserializeSample(data);
    Assert.assertEquals(sample, object);
  }

  @Test
  public void testSample() {
    MediaContent object = new MediaContent().populate(false);
    byte[] data = ProtoBuffersState.serializeMediaContent(object);
    MediaContent mediaContent = ProtoBuffersState.deserializeMediaContent(data);
    Assert.assertEquals(mediaContent, object);
  }
}
