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

import java.sql.Time;
import java.sql.Timestamp;
import java.time.Duration;
import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
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
import java.util.TimeZone;
import org.apache.fury.Fury;
import org.apache.fury.memory.MemoryBuffer;
import org.apache.fury.type.Type;
import org.apache.fury.util.DateTimeUtils;

/** Serializers for all time related types. */
public class TimeSerializers {
  public abstract static class TimeSerializer<T> extends Serializer<T> {

    public TimeSerializer(Fury fury, Class<T> type) {
      super(fury, type, !fury.getConfig().isTimeRefIgnored());
    }

    public TimeSerializer(Fury fury, Class<T> type, boolean needToWriteRef) {
      super(fury, type, needToWriteRef);
    }
  }

  public abstract static class BaseDateSerializer<T extends Date> extends TimeSerializer<T> {
    public BaseDateSerializer(Fury fury, Class<T> type) {
      super(fury, type);
    }

    public BaseDateSerializer(Fury fury, Class<T> type, boolean needToWriteRef) {
      super(fury, type, needToWriteRef);
    }

    @Override
    public void write(MemoryBuffer buffer, T value) {
      buffer.writeInt64(value.getTime());
    }

    @Override
    public T read(MemoryBuffer buffer) {
      return newInstance(buffer.readInt64());
    }

    protected abstract T newInstance(long time);
  }

  public static final class DateSerializer extends BaseDateSerializer<Date> {
    public DateSerializer(Fury fury) {
      super(fury, Date.class);
    }

    public DateSerializer(Fury fury, boolean needToWriteRef) {
      super(fury, Date.class, needToWriteRef);
    }

    @Override
    protected Date newInstance(long time) {
      return new Date(time);
    }
  }

  public static final class SqlDateSerializer extends BaseDateSerializer<java.sql.Date> {
    public SqlDateSerializer(Fury fury) {
      super(fury, java.sql.Date.class);
    }

    public SqlDateSerializer(Fury fury, boolean needToWriteRef) {
      super(fury, java.sql.Date.class, needToWriteRef);
    }

    @Override
    protected java.sql.Date newInstance(long time) {
      return new java.sql.Date(time);
    }
  }

  public static final class SqlTimeSerializer extends BaseDateSerializer<Time> {

    public SqlTimeSerializer(Fury fury) {
      super(fury, Time.class);
    }

    public SqlTimeSerializer(Fury fury, boolean needToWriteRef) {
      super(fury, Time.class, needToWriteRef);
    }

    @Override
    protected Time newInstance(long time) {
      return new Time(time);
    }
  }

  public static final class TimestampSerializer extends TimeSerializer<Timestamp> {
    private final short typeId;

    public TimestampSerializer(Fury fury) {
      // conflict with instant
      super(fury, Timestamp.class);
      typeId = (short) -Type.TIMESTAMP.getId();
    }

    public TimestampSerializer(Fury fury, boolean needToWriteRef) {
      super(fury, Timestamp.class, needToWriteRef);
      typeId = (short) -Type.TIMESTAMP.getId();
    }

    @Override
    public void xwrite(MemoryBuffer buffer, Timestamp value) {
      buffer.writeInt64(DateTimeUtils.fromJavaTimestamp(value));
    }

    @Override
    public Timestamp xread(MemoryBuffer buffer) {
      return DateTimeUtils.toJavaTimestamp(buffer.readInt64());
    }

    @Override
    public short getXtypeId() {
      return typeId;
    }

    @Override
    public void write(MemoryBuffer buffer, Timestamp value) {
      long time = value.getTime() - (value.getNanos() / 1_000_000);
      buffer.writeInt64(time);
      buffer.writeInt32(value.getNanos());
    }

    @Override
    public Timestamp read(MemoryBuffer buffer) {
      Timestamp t = new Timestamp(buffer.readInt64());
      t.setNanos(buffer.readInt32());
      return t;
    }
  }

  public static final class LocalDateSerializer extends TimeSerializer<LocalDate> {
    public LocalDateSerializer(Fury fury) {
      super(fury, LocalDate.class);
    }

    public LocalDateSerializer(Fury fury, boolean needToWriteRef) {
      super(fury, LocalDate.class, needToWriteRef);
    }

    @Override
    public short getXtypeId() {
      return Type.DATE32.getId();
    }

    @Override
    public void xwrite(MemoryBuffer buffer, LocalDate value) {
      // TODO use java encoding to support larger range.
      buffer.writeInt32(DateTimeUtils.localDateToDays(value));
    }

    @Override
    public LocalDate xread(MemoryBuffer buffer) {
      return DateTimeUtils.daysToLocalDate(buffer.readInt32());
    }

    @Override
    public void write(MemoryBuffer buffer, LocalDate value) {
      writeLocalDate(buffer, value);
    }

    public static void writeLocalDate(MemoryBuffer buffer, LocalDate value) {
      buffer.writeInt32(value.getYear());
      buffer.writeByte(value.getMonthValue());
      buffer.writeByte(value.getDayOfMonth());
    }

    @Override
    public LocalDate read(MemoryBuffer buffer) {
      return readLocalDate(buffer);
    }

    public static LocalDate readLocalDate(MemoryBuffer buffer) {
      int year = buffer.readInt32();
      int month = buffer.readByte();
      int dayOfMonth = buffer.readByte();
      return LocalDate.of(year, month, dayOfMonth);
    }
  }

  public static final class InstantSerializer extends TimeSerializer<Instant> {
    public InstantSerializer(Fury fury) {
      super(fury, Instant.class);
    }

    public InstantSerializer(Fury fury, boolean needToWriteRef) {
      super(fury, Instant.class, needToWriteRef);
    }

    @Override
    public short getXtypeId() {
      return Type.TIMESTAMP.getId();
    }

    @Override
    public void xwrite(MemoryBuffer buffer, Instant value) {
      // FIXME JDK17 may have higher precision than millisecond
      buffer.writeInt64(DateTimeUtils.instantToMicros(value));
    }

    @Override
    public Instant xread(MemoryBuffer buffer) {
      return DateTimeUtils.microsToInstant(buffer.readInt64());
    }

    @Override
    public void write(MemoryBuffer buffer, Instant value) {
      buffer.writeInt64(value.getEpochSecond());
      buffer.writeInt32(value.getNano());
    }

    @Override
    public Instant read(MemoryBuffer buffer) {
      long seconds = buffer.readInt64();
      int nanos = buffer.readInt32();
      return Instant.ofEpochSecond(seconds, nanos);
    }
  }

  public static class DurationSerializer extends TimeSerializer<Duration> {
    public DurationSerializer(Fury fury) {
      super(fury, Duration.class);
    }

    public DurationSerializer(Fury fury, boolean needToWriteRef) {
      super(fury, Duration.class, needToWriteRef);
    }

    @Override
    public void write(MemoryBuffer buffer, Duration value) {
      buffer.writeInt64(value.getSeconds());
      buffer.writeInt32(value.getNano());
    }

    @Override
    public Duration read(MemoryBuffer buffer) {
      long seconds = buffer.readInt64();
      int nanos = buffer.readInt32();
      return Duration.ofSeconds(seconds, nanos);
    }
  }

  public static class LocalDateTimeSerializer extends TimeSerializer<LocalDateTime> {
    public LocalDateTimeSerializer(Fury fury) {
      super(fury, LocalDateTime.class);
    }

    public LocalDateTimeSerializer(Fury fury, boolean needToWriteRef) {
      super(fury, LocalDateTime.class, needToWriteRef);
    }

    @Override
    public void write(MemoryBuffer buffer, LocalDateTime value) {
      LocalDateSerializer.writeLocalDate(buffer, value.toLocalDate());
      LocalTimeSerializer.writeLocalTime(buffer, value.toLocalTime());
    }

    @Override
    public LocalDateTime read(MemoryBuffer buffer) {
      LocalDate date = LocalDateSerializer.readLocalDate(buffer);
      LocalTime time = LocalTimeSerializer.readLocalTime(buffer);
      return LocalDateTime.of(date, time);
    }
  }

  public static class LocalTimeSerializer extends TimeSerializer<LocalTime> {
    public LocalTimeSerializer(Fury fury) {
      super(fury, LocalTime.class);
    }

    public LocalTimeSerializer(Fury fury, boolean needToWriteRef) {
      super(fury, LocalTime.class, needToWriteRef);
    }

    @Override
    public void write(MemoryBuffer buffer, LocalTime time) {
      writeLocalTime(buffer, time);
    }

    static void writeLocalTime(MemoryBuffer buffer, LocalTime time) {
      if (time.getNano() == 0) {
        if (time.getSecond() == 0) {
          if (time.getMinute() == 0) {
            buffer.writeByte(~time.getHour());
          } else {
            buffer.writeByte(time.getHour());
            buffer.writeByte(~time.getMinute());
          }
        } else {
          buffer.writeByte(time.getHour());
          buffer.writeByte(time.getMinute());
          buffer.writeByte(~time.getSecond());
        }
      } else {
        buffer.writeByte(time.getHour());
        buffer.writeByte(time.getMinute());
        buffer.writeByte(time.getSecond());
        buffer.writeInt32(time.getNano());
      }
    }

    @Override
    public LocalTime read(MemoryBuffer buffer) {
      return readLocalTime(buffer);
    }

    static LocalTime readLocalTime(MemoryBuffer buffer) {
      int hour = buffer.readByte();
      int minute = 0;
      int second = 0;
      int nano = 0;
      if (hour < 0) {
        hour = ~hour;
      } else {
        minute = buffer.readByte();
        if (minute < 0) {
          minute = ~minute;
        } else {
          second = buffer.readByte();
          if (second < 0) {
            second = ~second;
          } else {
            nano = buffer.readInt32();
          }
        }
      }
      return LocalTime.of(hour, minute, second, nano);
    }
  }

  public static class TimeZoneSerializer extends TimeSerializer<TimeZone> {
    public TimeZoneSerializer(Fury fury, Class<TimeZone> type) {
      super(fury, type);
    }

    public TimeZoneSerializer(Fury fury, boolean needToWriteRef) {
      super(fury, TimeZone.class, needToWriteRef);
    }

    public void write(MemoryBuffer buffer, TimeZone object) {
      fury.writeJavaString(buffer, object.getID());
    }

    public TimeZone read(MemoryBuffer buffer) {
      return TimeZone.getTimeZone(fury.readJavaString(buffer));
    }
  }

  public static final class CalendarSerializer extends TimeSerializer<Calendar> {
    private static final long DEFAULT_GREGORIAN_CUTOVER = -12219292800000L;
    private final TimeZoneSerializer timeZoneSerializer;

    public CalendarSerializer(Fury fury, Class<Calendar> type) {
      super(fury, type);
      timeZoneSerializer = new TimeZoneSerializer(fury, TimeZone.class);
    }

    public CalendarSerializer(Fury fury, Class<Calendar> type, boolean needToWriteRef) {
      super(fury, type, needToWriteRef);
      timeZoneSerializer = new TimeZoneSerializer(fury, TimeZone.class);
    }

    public void write(MemoryBuffer buffer, Calendar object) {
      timeZoneSerializer.write(buffer, object.getTimeZone()); // can't be null
      buffer.writeInt64(object.getTimeInMillis());
      buffer.writeBoolean(object.isLenient());
      buffer.writeByte(object.getFirstDayOfWeek());
      buffer.writeByte(object.getMinimalDaysInFirstWeek());
      if (object instanceof GregorianCalendar) {
        buffer.writeInt64(((GregorianCalendar) object).getGregorianChange().getTime());
      } else {
        buffer.writeInt64(DEFAULT_GREGORIAN_CUTOVER);
      }
    }

    public Calendar read(MemoryBuffer buffer) {
      Calendar result = Calendar.getInstance(timeZoneSerializer.read(buffer));
      result.setTimeInMillis(buffer.readInt64());
      result.setLenient(buffer.readBoolean());
      result.setFirstDayOfWeek(buffer.readByte());
      result.setMinimalDaysInFirstWeek(buffer.readByte());
      long gregorianChange = buffer.readInt64();
      if (gregorianChange != DEFAULT_GREGORIAN_CUTOVER) {
        if (result instanceof GregorianCalendar) {
          ((GregorianCalendar) result).setGregorianChange(new Date(gregorianChange));
        }
      }
      return result;
    }
  }

  public static class ZoneIdSerializer extends TimeSerializer<ZoneId> {
    public ZoneIdSerializer(Fury fury, Class<ZoneId> type) {
      super(fury, type);
    }

    public ZoneIdSerializer(Fury fury, Class<ZoneId> type, boolean needToWriteRef) {
      super(fury, type, needToWriteRef);
    }

    @Override
    public void write(MemoryBuffer buffer, ZoneId obj) {
      fury.writeString(buffer, obj.getId());
    }

    @Override
    public ZoneId read(MemoryBuffer buffer) {
      return ZoneId.of(fury.readString(buffer));
    }
  }

  public static class ZoneOffsetSerializer extends TimeSerializer<ZoneOffset> {
    public ZoneOffsetSerializer(Fury fury) {
      super(fury, ZoneOffset.class);
    }

    public ZoneOffsetSerializer(Fury fury, boolean needToWriteRef) {
      super(fury, ZoneOffset.class, needToWriteRef);
    }

    public void write(MemoryBuffer buffer, ZoneOffset obj) {
      writeZoneOffset(buffer, obj);
    }

    public static void writeZoneOffset(MemoryBuffer buffer, ZoneOffset obj) {
      final int offsetSecs = obj.getTotalSeconds();
      int offsetByte = offsetSecs % 900 == 0 ? offsetSecs / 900 : 127; // compress to -72 to +72
      buffer.writeByte(offsetByte);
      if (offsetByte == 127) {
        buffer.writeInt32(offsetSecs);
      }
    }

    public ZoneOffset read(MemoryBuffer buffer) {
      return readZoneOffset(buffer);
    }

    public static ZoneOffset readZoneOffset(MemoryBuffer buffer) {
      int offsetByte = buffer.readByte();
      return (offsetByte == 127
          ? ZoneOffset.ofTotalSeconds(buffer.readInt32())
          : ZoneOffset.ofTotalSeconds(offsetByte * 900));
    }
  }

  public static class ZonedDateTimeSerializer extends TimeSerializer<ZonedDateTime> {

    public ZonedDateTimeSerializer(Fury fury) {
      super(fury, ZonedDateTime.class);
    }

    public ZonedDateTimeSerializer(Fury fury, boolean needToWriteRef) {
      super(fury, ZonedDateTime.class, needToWriteRef);
    }

    public void write(MemoryBuffer buffer, ZonedDateTime obj) {
      LocalDateSerializer.writeLocalDate(buffer, obj.toLocalDate());
      LocalTimeSerializer.writeLocalTime(buffer, obj.toLocalTime());
      fury.writeString(buffer, obj.getZone().getId());
    }

    public ZonedDateTime read(MemoryBuffer buffer) {
      LocalDate date = LocalDateSerializer.readLocalDate(buffer);
      LocalTime time = LocalTimeSerializer.readLocalTime(buffer);
      ZoneId zone = ZoneId.of(fury.readString(buffer));
      return ZonedDateTime.of(date, time, zone);
    }
  }

  public static class YearSerializer extends TimeSerializer<Year> {
    public YearSerializer(Fury fury) {
      super(fury, Year.class);
    }

    public YearSerializer(Fury fury, boolean needToWriteRef) {
      super(fury, Year.class, needToWriteRef);
    }

    @Override
    public void write(MemoryBuffer buffer, Year obj) {
      buffer.writeInt32(obj.getValue());
    }

    @Override
    public Year read(MemoryBuffer buffer) {
      return Year.of(buffer.readInt32());
    }
  }

  public static class YearMonthSerializer extends TimeSerializer<YearMonth> {
    public YearMonthSerializer(Fury fury) {
      super(fury, YearMonth.class);
    }

    public YearMonthSerializer(Fury fury, boolean needToWriteRef) {
      super(fury, YearMonth.class, needToWriteRef);
    }

    @Override
    public void write(MemoryBuffer buffer, YearMonth obj) {
      buffer.writeInt32(obj.getYear());
      buffer.writeByte(obj.getMonthValue());
    }

    @Override
    public YearMonth read(MemoryBuffer buffer) {
      int year = buffer.readInt32();
      byte month = buffer.readByte();
      return YearMonth.of(year, month);
    }
  }

  public static class MonthDaySerializer extends TimeSerializer<MonthDay> {
    public MonthDaySerializer(Fury fury) {
      super(fury, MonthDay.class);
    }

    public MonthDaySerializer(Fury fury, boolean needToWriteRef) {
      super(fury, MonthDay.class, needToWriteRef);
    }

    public void write(MemoryBuffer buffer, MonthDay obj) {
      buffer.writeByte(obj.getMonthValue());
      buffer.writeByte(obj.getDayOfMonth());
    }

    public MonthDay read(MemoryBuffer buffer) {
      byte month = buffer.readByte();
      byte day = buffer.readByte();
      return MonthDay.of(month, day);
    }
  }

  public static class PeriodSerializer extends TimeSerializer<Period> {
    public PeriodSerializer(Fury fury) {
      super(fury, Period.class);
    }

    public PeriodSerializer(Fury fury, boolean needToWriteRef) {
      super(fury, Period.class, needToWriteRef);
    }

    public void write(MemoryBuffer buffer, Period obj) {
      buffer.writeInt32(obj.getYears());
      buffer.writeInt32(obj.getMonths());
      buffer.writeInt32(obj.getDays());
    }

    public Period read(MemoryBuffer buffer) {
      int years = buffer.readInt32();
      int months = buffer.readInt32();
      int days = buffer.readInt32();
      return Period.of(years, months, days);
    }
  }

  public static class OffsetTimeSerializer extends TimeSerializer<OffsetTime> {
    public OffsetTimeSerializer(Fury fury) {
      super(fury, OffsetTime.class);
    }

    public OffsetTimeSerializer(Fury fury, boolean needToWriteRef) {
      super(fury, OffsetTime.class, needToWriteRef);
    }

    public void write(MemoryBuffer buffer, OffsetTime obj) {
      LocalTimeSerializer.writeLocalTime(buffer, obj.toLocalTime());
      ZoneOffsetSerializer.writeZoneOffset(buffer, obj.getOffset());
    }

    public OffsetTime read(MemoryBuffer buffer) {
      LocalTime time = LocalTimeSerializer.readLocalTime(buffer);
      ZoneOffset offset = ZoneOffsetSerializer.readZoneOffset(buffer);
      return OffsetTime.of(time, offset);
    }
  }

  public static class OffsetDateTimeSerializer extends TimeSerializer<OffsetDateTime> {
    public OffsetDateTimeSerializer(Fury fury) {
      super(fury, OffsetDateTime.class);
    }

    public OffsetDateTimeSerializer(Fury fury, boolean needToWriteRef) {
      super(fury, OffsetDateTime.class, needToWriteRef);
    }

    public void write(MemoryBuffer buffer, OffsetDateTime obj) {
      LocalDateSerializer.writeLocalDate(buffer, obj.toLocalDate());
      LocalTimeSerializer.writeLocalTime(buffer, obj.toLocalTime());
      ZoneOffsetSerializer.writeZoneOffset(buffer, obj.getOffset());
    }

    public OffsetDateTime read(MemoryBuffer buffer) {
      LocalDate date = LocalDateSerializer.readLocalDate(buffer);
      LocalTime time = LocalTimeSerializer.readLocalTime(buffer);
      ZoneOffset offset = ZoneOffsetSerializer.readZoneOffset(buffer);
      return OffsetDateTime.of(date, time, offset);
    }
  }

  public static void registerDefaultSerializers(Fury fury) {
    fury.registerSerializer(Date.class, new DateSerializer(fury));
    fury.registerSerializer(java.sql.Date.class, new SqlDateSerializer(fury));
    fury.registerSerializer(Time.class, new SqlTimeSerializer(fury));
    fury.registerSerializer(Timestamp.class, new TimestampSerializer(fury));
    fury.registerSerializer(LocalDate.class, new LocalDateSerializer(fury));
    fury.registerSerializer(LocalTime.class, new LocalTimeSerializer(fury));
    fury.registerSerializer(LocalDateTime.class, new LocalDateTimeSerializer(fury));
    fury.registerSerializer(Instant.class, new InstantSerializer(fury));
    fury.registerSerializer(Duration.class, new DurationSerializer(fury));
    fury.registerSerializer(ZoneOffset.class, new ZoneOffsetSerializer(fury));
    fury.registerSerializer(ZonedDateTime.class, new ZonedDateTimeSerializer(fury));
    fury.registerSerializer(Year.class, new YearSerializer(fury));
    fury.registerSerializer(YearMonth.class, new YearMonthSerializer(fury));
    fury.registerSerializer(MonthDay.class, new MonthDaySerializer(fury));
    fury.registerSerializer(Period.class, new PeriodSerializer(fury));
    fury.registerSerializer(OffsetTime.class, new OffsetTimeSerializer(fury));
    fury.registerSerializer(OffsetDateTime.class, new OffsetDateTimeSerializer(fury));
  }
}
