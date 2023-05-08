/*
 * Copyright 2023 The Fury authors
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package io.fury.serializer;

import io.fury.Fury;
import io.fury.FuryTestBase;
import io.fury.Language;
import io.fury.util.DateTimeUtils;
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
import org.testng.annotations.Test;

public class TimeSerializersTest extends FuryTestBase {

  @Test
  public void testBasicTime() {
    Fury fury = Fury.builder().withLanguage(Language.JAVA).disableSecureMode().build();
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
    Fury fury = Fury.builder().withLanguage(Language.JAVA).disableSecureMode().build();
    serDeCheckSerializerAndEqual(fury, GregorianCalendar.getInstance(), "Calendar");
    serDeCheckSerializerAndEqual(fury, Calendar.getInstance(), "Calendar");
  }

  @Test
  public void testZone() {
    Fury fury = Fury.builder().withLanguage(Language.JAVA).disableSecureMode().build();
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

  // TODO(chaokunyang) add time struct tests.
}
