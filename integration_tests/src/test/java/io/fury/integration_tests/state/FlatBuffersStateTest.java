package io.fury.integration_tests.state;

import io.fury.benchmark.data.MediaContent;
import io.fury.benchmark.data.Sample;
import java.nio.ByteBuffer;
import org.testng.Assert;
import org.testng.annotations.Test;

public class FlatBuffersStateTest {
  @Test
  public void testMediaContent() {
    Sample object = new Sample().populate(false);
    byte[] data = FlatBuffersState.serializeSample(object);
    Sample sample = FlatBuffersState.deserializeSample(ByteBuffer.wrap(data));
    Assert.assertEquals(sample, object);
  }

  @Test
  public void testSample() {
    MediaContent object = new MediaContent().populate(false);
    byte[] data = FlatBuffersState.serializeMediaContent(object).sizedByteArray();
    MediaContent mediaContent = FlatBuffersState.deserializeMediaContent(ByteBuffer.wrap(data));
    Assert.assertEquals(mediaContent, object);
  }
}
