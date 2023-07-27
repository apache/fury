/*
 * Copyright 2023 The Fury Authors
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package io.fury.benchmark.data;

import java.util.ArrayList;
import java.util.List;

public class MediaContent implements java.io.Serializable {
  public Media media;
  public List<Image> images;

  public MediaContent() {}

  public MediaContent(Media media, List<Image> images) {
    this.media = media;
    this.images = images;
  }

  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    MediaContent other = (MediaContent) o;
    if (images != null ? !images.equals(other.images) : other.images != null) {
      return false;
    }
    if (media != null ? !media.equals(other.media) : other.media != null) {
      return false;
    }
    return true;
  }

  public int hashCode() {
    int result = media != null ? media.hashCode() : 0;
    result = 31 * result + (images != null ? images.hashCode() : 0);
    return result;
  }

  public String toString() {
    StringBuilder sb = new StringBuilder();
    sb.append("[MediaContent: ");
    sb.append("media=").append(media);
    sb.append(", images=").append(images);
    sb.append("]");
    return sb.toString();
  }

  public MediaContent populate(boolean circularReference) {
    media = new Media();
    media.uri = "http://javaone.com/keynote.ogg";
    media.width = 641;
    media.height = 481;
    media.format = "video/theora\u1234";
    media.duration = 18000001;
    media.size = 58982401;
    media.persons = new ArrayList();
    media.persons.add("Bill Gates, Jr.");
    media.persons.add("Steven Jobs");
    media.player = Media.Player.FLASH;
    media.copyright = "Copyright (c) 2009, Scooby Dooby Doo";
    images = new ArrayList();
    Media media = circularReference ? this.media : null;
    images.add(
        new Image(
            "http://javaone.com/keynote_huge.jpg",
            "Javaone Keynote\u1234",
            32000,
            24000,
            Image.Size.LARGE,
            media));
    images.add(
        new Image(
            "http://javaone.com/keynote_large.jpg", null, 1024, 768, Image.Size.LARGE, media));
    images.add(
        new Image("http://javaone.com/keynote_small.jpg", null, 320, 240, Image.Size.SMALL, media));
    return this;
  }
}
