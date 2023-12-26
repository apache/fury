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

package org.apache.fury.serializer;

import java.sql.Timestamp;
import java.time.Duration;
import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.Month;
import java.time.MonthDay;
import java.time.OffsetDateTime;
import java.time.OffsetTime;
import java.time.Period;
import java.time.Year;
import java.time.YearMonth;
import java.time.ZoneId;
import java.time.ZoneOffset;
import java.time.ZonedDateTime;
import java.util.Calendar;
import java.util.Date;
import java.util.GregorianCalendar;
import lombok.Data;
import org.apache.fury.Fury;
import org.apache.fury.FuryTestBase;
import org.apache.fury.config.Language;
import org.apache.fury.util.DateTimeUtils;
import org.testng.Assert;
import org.testng.annotations.Test;

public class TimeSerializersTest extends FuryTestBase {

  @Test
  public void testBasicTime() {
    Fury fury = Fury.builder().withLanguage(Language.JAVA).requireClassRegistration(false).build();
    serDeCheckSerializerAndEqual(fury, new Date(), "Time");
    serDeCheckSerializerAndEqual(fury, new java.sql.Date(100), "Time");
    serDeCheckSerializerAndEqual(fury, new java.sql.Time(200), "Time");
    serDeCheckSerializerAndEqual(fury, new Timestamp(300), "Time");
    serDeCheckSerializerAndEqual(fury, new java.sql.Date(-100), "Time");
    serDeCheckSerializerAndEqual(fury, new java.sql.Time(-200), "Time");
    serDeCheckSerializerAndEqual(fury, new Timestamp(-300), "Time");
    serDeCheckSerializerAndEqual(fury, LocalDate.now(), "Time");
    serDeCheckSerializerAndEqual(fury, LocalTime.now(), "Time");
    serDeCheckSerializerAndEqual(fury, LocalDateTime.now(), "Time");
    serDeCheckSerializerAndEqual(
        fury, DateTimeUtils.truncateInstantToMicros(Instant.now()), "Time");
    serDeCheckSerializerAndEqual(
        fury, Duration.between((Instant.now()), Instant.ofEpochSecond(-1)), "Time");
    serDeCheckSerializerAndEqual(fury, Period.of(100, 11, 20), "Time");
  }

  @Test
  public void testCalendar() {
    Fury fury = Fury.builder().withLanguage(Language.JAVA).requireClassRegistration(false).build();
    serDeCheckSerializerAndEqual(fury, GregorianCalendar.getInstance(), "Calendar");
    serDeCheckSerializerAndEqual(fury, Calendar.getInstance(), "Calendar");
  }

  @Test
  public void testZone() {
    Fury fury = Fury.builder().withLanguage(Language.JAVA).requireClassRegistration(false).build();
    serDeCheckSerializerAndEqual(
        fury,
        ZonedDateTime.of(Year.MIN_VALUE, Month.JANUARY.getValue(), 1, 0, 0, 0, 0, ZoneOffset.UTC),
        "ZonedDateTimeSerializer");
    serDeCheckSerializerAndEqual(
        fury,
        ZonedDateTime.of(
            Year.MIN_VALUE, Month.JANUARY.getValue(), 1, 0, 0, 0, 0, ZoneId.of("Europe/Berlin")),
        "ZonedDateTimeSerializer");
    serDeCheckSerializerAndEqual(
        fury,
        ZonedDateTime.of(
            Year.MAX_VALUE,
            Month.DECEMBER.getValue(),
            31,
            23,
            59,
            59,
            999999999,
            ZoneId.of("Europe/Berlin")),
        "ZonedDateTimeSerializer");
    serDeCheckSerializerAndEqual(fury, Year.of(Year.MIN_VALUE), "YearSerializer");
    serDeCheckSerializerAndEqual(fury, Year.of(Year.MAX_VALUE), "YearSerializer");
    serDeCheckSerializerAndEqual(
        fury, YearMonth.of(Year.MIN_VALUE, Month.APRIL), "YearMonthSerializer");
    serDeCheckSerializerAndEqual(
        fury, YearMonth.of(Year.MAX_VALUE, Month.APRIL), "YearMonthSerializer");
    serDeCheckSerializerAndEqual(fury, MonthDay.of(Month.JANUARY, 11), "MonthDaySerializer");
    serDeCheckSerializerAndEqual(fury, MonthDay.of(Month.DECEMBER, 11), "MonthDaySerializer");
    serDeCheckSerializerAndEqual(
        fury, OffsetTime.of(1, 1, 1, 1, ZoneOffset.UTC), "OffsetTimeSerializer");
    serDeCheckSerializerAndEqual(
        fury, OffsetTime.of(23, 59, 59, 999999999, ZoneOffset.UTC), "OffsetTimeSerializer");
    serDeCheckSerializerAndEqual(
        fury,
        OffsetDateTime.of(Year.MIN_VALUE, Month.JANUARY.getValue(), 1, 0, 0, 0, 0, ZoneOffset.UTC),
        "OffsetDateTimeSerializer");
    serDeCheckSerializerAndEqual(
        fury,
        OffsetDateTime.of(
            Year.MAX_VALUE, Month.DECEMBER.getValue(), 31, 23, 59, 59, 999999999, ZoneOffset.UTC),
        "OffsetDateTimeSerializer");
  }

  @Data
  public static class TimeStruct {
    Date date;
    java.sql.Date sqlDate;
    java.sql.Time time;
    Timestamp timestamp;
    LocalDate localDate;
    LocalTime localTime;
    LocalDateTime localDateTime;
    Instant instant;
    Duration duration;
  }

  @Test
  public void testTimeStruct() {
    TimeStruct struct = new TimeStruct();
    struct.date = new Date();
    struct.sqlDate = new java.sql.Date(100);
    struct.time = new java.sql.Time(200);
    struct.timestamp = new Timestamp(300);
    struct.localDate = LocalDate.now();
    struct.localTime = LocalTime.now();
    struct.localDateTime = LocalDateTime.now();
    struct.instant = DateTimeUtils.truncateInstantToMicros(Instant.now());
    struct.duration = Duration.between(Instant.now(), Instant.ofEpochSecond(-1));
    {
      Fury fury =
          Fury.builder().withLanguage(Language.JAVA).requireClassRegistration(false).build();
      fury.registerSerializer(
          TimeStruct.class, CodegenSerializer.loadCodegenSerializer(fury, TimeStruct.class));
      serDe(fury, struct);
    }
    {
      Fury fury =
          Fury.builder().withLanguage(Language.JAVA).requireClassRegistration(false).build();
      fury.registerSerializer(TimeStruct.class, new ObjectSerializer(fury, TimeStruct.class));
      serDe(fury, struct);
    }
  }

  @Data
  public static class TimeStructRef {
    Date date1;
    Date date2;
    java.sql.Date sqlDate1;
    java.sql.Date sqlDate2;
    java.sql.Time time1;
    java.sql.Time time2;
    Instant instant1;
    Instant instant2;
    Duration duration1;
    Duration duration2;
  }

  public static class TimeStructRef1 extends TimeStructRef {}

  public static class TimeStructRef2 extends TimeStructRef {}

  private TimeStructRef createTimeStructRef(TimeStructRef struct) {
    struct.date1 = new Date();
    struct.date2 = struct.date1;
    struct.sqlDate1 = new java.sql.Date(100);
    struct.sqlDate2 = struct.sqlDate1;
    struct.time1 = new java.sql.Time(200);
    struct.time2 = struct.time1;
    struct.instant1 = DateTimeUtils.truncateInstantToMicros(Instant.now());
    struct.instant2 = struct.instant1;
    struct.duration1 = Duration.between(Instant.now(), Instant.ofEpochSecond(-1));
    struct.duration2 = struct.duration1;
    return struct;
  }

  @Test
  public void testTimeStructRef() {
    {
      Fury fury =
          Fury.builder()
              .withLanguage(Language.JAVA)
              .requireClassRegistration(false)
              .withCodegen(true)
              .withRefTracking(true)
              .ignoreTimeRef(true)
              .build();
      fury.registerSerializer(
          TimeStructRef.class, CodegenSerializer.loadCodegenSerializer(fury, TimeStructRef.class));
      TimeStructRef struct = createTimeStructRef(new TimeStructRef());
      TimeStructRef struct1 = (TimeStructRef) serDeCheck(fury, struct);
      Assert.assertNotSame(struct1.date1, struct1.date2);
      Assert.assertNotSame(struct1.sqlDate1, struct1.sqlDate2);
      Assert.assertNotSame(struct1.time1, struct1.time2);
      Assert.assertNotSame(struct1.instant1, struct1.instant2);
      Assert.assertNotSame(struct1.duration1, struct1.duration2);
    }
    {
      Fury fury =
          Fury.builder()
              .withLanguage(Language.JAVA)
              .requireClassRegistration(false)
              .withRefTracking(true)
              .ignoreTimeRef(false)
              .build();
      fury.registerSerializer(
          TimeStruct.class, CodegenSerializer.loadCodegenSerializer(fury, TimeStruct.class));
      fury.registerSerializer(
          TimeStructRef1.class,
          CodegenSerializer.loadCodegenSerializer(fury, TimeStructRef1.class));
      TimeStructRef1 struct = (TimeStructRef1) createTimeStructRef(new TimeStructRef1());
      TimeStructRef1 struct1 = (TimeStructRef1) serDeCheck(fury, struct);
      Assert.assertSame(struct1.date1, struct1.date2);
      Assert.assertSame(struct1.sqlDate1, struct1.sqlDate2);
      Assert.assertSame(struct1.time1, struct1.time2);
      Assert.assertSame(struct1.instant1, struct1.instant2);
      Assert.assertSame(struct1.duration1, struct1.duration2);
    }
    {
      Fury fury =
          Fury.builder()
              .withLanguage(Language.JAVA)
              .requireClassRegistration(false)
              .withCodegen(true)
              .withRefTracking(true)
              .ignoreTimeRef(true)
              .build();
      fury.registerSerializer(Date.class, new TimeSerializers.DateSerializer(fury, true));
      fury.registerSerializer(
          java.sql.Date.class, new TimeSerializers.SqlDateSerializer(fury, true));
      fury.registerSerializer(Instant.class, new TimeSerializers.InstantSerializer(fury, true));
      {
        TimeStructRef struct = createTimeStructRef(new TimeStructRef());
        TimeStructRef struct2 = (TimeStructRef) serDeCheck(fury, struct);
        // TimeStructRef serializer already generated, enable ref tracking doesn't take effect.
        Assert.assertNotSame(struct2.date1, struct2.date2);
      }
      {
        TimeStructRef struct = createTimeStructRef(new TimeStructRef2());
        TimeStructRef struct2 = (TimeStructRef) serDeCheck(fury, struct);
        Assert.assertSame(struct2.date1, struct2.date2);
        Assert.assertSame(struct2.sqlDate1, struct2.sqlDate2);
        Assert.assertSame(struct2.instant1, struct2.instant2);
        Assert.assertNotSame(struct2.time1, struct2.time2);
        Assert.assertNotSame(struct2.duration1, struct2.duration2);
      }
    }
  }
}
