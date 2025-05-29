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

package org.apache.fory.util;

import static java.util.concurrent.TimeUnit.DAYS;
import static java.util.concurrent.TimeUnit.MICROSECONDS;
import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static java.util.concurrent.TimeUnit.NANOSECONDS;
import static java.util.concurrent.TimeUnit.SECONDS;

import java.sql.Date;
import java.sql.Timestamp;
import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.util.TimeZone;

public class DateTimeUtils {

  public static TimeZone DEFAULT_TIMEZONE = TimeZone.getDefault();
  public static long MICROS_PER_SECOND = SECONDS.toMicros(1);
  public static long MILLIS_PER_DAY = DAYS.toMillis(1);
  public static long MICROS_PER_MILLIS = 1000;
  public static final long NANOS_PER_MICROS = MICROSECONDS.toNanos(1);
  public static final long NANOS_PER_MILLIS = NANOS_PER_MICROS * MICROS_PER_MILLIS;

  public static int localDateToDays(LocalDate localDate) {
    return Math.toIntExact(localDate.toEpochDay());
  }

  public static int fromJavaDate(Date date) {
    return millisToDays(date.getTime());
  }

  public static int millisToDays(long millisUtc) {
    return millisToDays(millisUtc, DEFAULT_TIMEZONE);
  }

  public static int millisToDays(long millisUtc, TimeZone timeZone) {
    long millisLocal = millisUtc + timeZone.getOffset(millisUtc);
    return (int) Math.floorDiv(millisLocal, MILLIS_PER_DAY);
  }

  public static long fromJavaTimestamp(Timestamp t) {
    return instantToMicros(t.toInstant());
  }

  public static long instantToMicros(Instant instant) {
    long us = Math.multiplyExact(instant.getEpochSecond(), MICROS_PER_SECOND);
    return Math.addExact(us, NANOSECONDS.toMicros(instant.getNano()));
  }

  public static Instant truncateInstantToMicros(Instant instant) {
    return Instant.ofEpochSecond(
        instant.getEpochSecond(),
        (int) (NANOSECONDS.toMicros(instant.getNano())) * NANOS_PER_MICROS);
  }

  // reverse of millisToDays
  public static long daysToMillis(int days) {
    return daysToMillis(days, DEFAULT_TIMEZONE);
  }

  public static long daysToMillis(int days, TimeZone timeZone) {
    long millisLocal = (long) days * MILLIS_PER_DAY;
    return millisLocal - getOffsetFromLocalMillis(millisLocal, timeZone);
  }

  /**
   * Lookup the offset for given millis seconds since 1970-01-01 00:00:00 in given timezone. TODO:
   * Improve handling of normalization differences. TODO: Replace with JSR-310 or similar system
   */
  private static int getOffsetFromLocalMillis(long millisLocal, TimeZone tz) {
    int guess = tz.getRawOffset();
    // the actual offset should be calculated based on milliseconds in UTC
    int offset = tz.getOffset(millisLocal - guess);
    if (offset != guess) {
      guess = tz.getOffset(millisLocal - offset);
      if (guess != offset) {
        // fallback to do the reverse lookup using java.time.LocalDateTime
        // this should only happen near the start or end of DST
        LocalDate localDate = LocalDate.ofEpochDay(MILLISECONDS.toDays(millisLocal));
        LocalTime localTime =
            LocalTime.ofNanoOfDay(MILLISECONDS.toNanos(Math.floorMod(millisLocal, MILLIS_PER_DAY)));
        LocalDateTime localDateTime = LocalDateTime.of(localDate, localTime);
        long millisEpoch = localDateTime.atZone(tz.toZoneId()).toInstant().toEpochMilli();

        guess = (int) (millisLocal - millisEpoch);
      }
    }
    return guess;
  }

  public static LocalDate daysToLocalDate(int days) {
    return LocalDate.ofEpochDay(days);
  }

  /** Returns a java.sql.Date from number of days since epoch. */
  public static Date toJavaDate(int daysSinceEpoch) {
    return new Date(daysToMillis(daysSinceEpoch));
  }

  public static Instant microsToInstant(long us) {
    long secs = Math.floorDiv(us, MICROS_PER_SECOND);
    long mos = MathUtils.floorMod(us, MICROS_PER_SECOND, secs);
    return Instant.ofEpochSecond(secs, mos * NANOS_PER_MICROS);
  }

  public static Timestamp toJavaTimestamp(long us) {
    return Timestamp.from(microsToInstant(us));
  }
}
