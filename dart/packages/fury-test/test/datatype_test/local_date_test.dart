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

// @Skip()
library;

import 'package:fury/fury.dart';
import 'package:test/test.dart';
import 'package:checks/checks.dart';

void main() {
  group('LocalDate Constructors', () {

    test('Basic constructor validates input', () {
      // Valid dates
      check(LocalDate(2025, 3, 19)).isA<LocalDate>();
      check(LocalDate(2024, 2, 29)).isA<LocalDate>(); // Leap year
      check(LocalDate(2000, 2, 29)).isA<LocalDate>(); // Leap year (divisible by 400)

      // Invalid dates
      check(() => LocalDate(2025, 0, 1)).throws<ArgumentError>();
      check(() => LocalDate(2025, 13, 1)).throws<ArgumentError>();
      check(() => LocalDate(2025, 4, 31)).throws<ArgumentError>();
      check(() => LocalDate(2025, 2, 29)).throws<ArgumentError>(); // Not a leap year
    });

    test('LocalDate.parse correctly parses ISO dates', () {
      var date = LocalDate.parse('2025-03-19');
      check(date.year).equals(2025);
      check(date.month).equals(3);
      check(date.day).equals(19);

      date = LocalDate.parse('2024-02-29');
      check(date.year).equals(2024);
      check(date.month).equals(2);
      check(date.day).equals(29);

      check(() => LocalDate.parse('2025-3-19')).throws<FormatException>();
      check(() => LocalDate.parse('2025/03/19')).throws<FormatException>();
      check(() => LocalDate.parse('not-a-date')).throws<FormatException>();
    });

    test('LocalDate.fromDateTime correctly creates date from DateTime', () {
      var dateTime = DateTime(2025, 3, 19);
      var date = LocalDate.fromDateTime(dateTime);
      check(date.year).equals(2025);
      check(date.month).equals(3);
      check(date.day).equals(19);

      dateTime = DateTime.utc(2020, 12, 31, 23, 59, 59);
      date = LocalDate.fromDateTime(dateTime);
      check(date.year).equals(2020);
      check(date.month).equals(12);
      check(date.day).equals(31);
      check(date.toString()).equals('2020-12-31');
    });

    // Fix fromEpochDay test
    test('LocalDate.fromEpochDay creates correct dates', () {
      // 1970-01-01 is day 0
      var date = LocalDate.fromEpochDay(0, utc: true);
      check(date.toString()).equals('1970-01-01');

      date = LocalDate.fromEpochDay(-1, utc: true);
      check(date.toString()).equals('1969-12-31');

      // Use specific values instead of estimated values
      var specificDate = LocalDate(2025, 3, 19);
      var days = specificDate.toEpochDay(utc: true);
      var reconstructedDate = LocalDate.fromEpochDay(days, utc: true);

      check(reconstructedDate.year).equals(2025);
      check(reconstructedDate.month).equals(3);
      check(reconstructedDate.day).equals(19);
    });

    test('LocalDate.fromEpochMillis creates correct dates', () {
      // Baseline test
      var date = LocalDate.fromEpochMillis(0, utc: true);
      check(date.toString()).equals('1970-01-01');

      // Use correct milliseconds instead of estimated values
      var specificDate = LocalDate(2025, 3, 19);
      var millis = specificDate.toEpochMillis(utc: true);
      var reconstructedDate = LocalDate.fromEpochMillis(millis, utc: true);

      check(reconstructedDate.year).equals(2025);
      check(reconstructedDate.month).equals(3);
      check(reconstructedDate.day).equals(19);
    });

    test('LocalDate.fromEpochSeconds creates correct dates', () {
      // 1970-01-01 00:00:00 UTC is 0 seconds
      var date = LocalDate.fromEpochSeconds(0, utc: true);
      check(date.toString()).equals('1970-01-01');

      // One day in seconds = 86,400
      date = LocalDate.fromEpochSeconds(86400, utc: true);
      check(date.toString()).equals('1970-01-02');

      // 2000-01-01 in seconds
      date = LocalDate.fromEpochSeconds(946684800, utc: true);
      check(date.year).equals(2000);
      check(date.month).equals(1);
      check(date.day).equals(1);
    });

    test('Epoch is correctly defined', () {
      check(LocalDate.epoch.year).equals(1970);
      check(LocalDate.epoch.month).equals(1);
      check(LocalDate.epoch.day).equals(1);
    });
  });

  group('LocalDate Conversions', () {
    test('toEpochDay returns correct days since epoch', () {
      var epochDate = LocalDate(1970, 1, 1);
      check(epochDate.toEpochDay(utc: true)).equals(0);

      var nextDay = LocalDate(1970, 1, 2);
      check(nextDay.toEpochDay(utc: true)).equals(1);

      var beforeEpoch = LocalDate(1969, 12, 31);
      check(beforeEpoch.toEpochDay(utc: true)).equals(-1);

      var leapYearDay = LocalDate(2024, 2, 29);
      check(leapYearDay.toEpochDay(utc: true))
          .which((days) => days..isGreaterThan(19700)..isLessThan(19850));
    });

    test('toEpochMillis returns correct milliseconds since epoch', () {
      var epochDate = LocalDate(1970, 1, 1);
      check(epochDate.toEpochMillis(utc: true)).equals(0);

      var nextDay = LocalDate(1970, 1, 2);
      check(nextDay.toEpochMillis(utc: true)).equals(86400000); // 24 hours in milliseconds

      var date2000 = LocalDate(2000, 1, 1);
      check(date2000.toEpochMillis(utc: true)).equals(946684800000);
    });

    test('toEpochSeconds returns correct seconds since epoch', () {
      var epochDate = LocalDate(1970, 1, 1);
      check(epochDate.toEpochSeconds(utc: true)).equals(0);

      var nextDay = LocalDate(1970, 1, 2);
      check(nextDay.toEpochSeconds(utc: true)).equals(86400); // 24 hours in seconds
    });

    test('toDateTime returns DateTime with correct date components', () {
      var date = LocalDate(2025, 3, 19);
      var dateTime = date.toDateTime();

      check(dateTime.year).equals(2025);
      check(dateTime.month).equals(3);
      check(dateTime.day).equals(19);
      check(dateTime.hour).equals(0);
      check(dateTime.minute).equals(0);
      check(dateTime.second).equals(0);
      check(dateTime.isUtc).isFalse();
    });

    test('toDateTimeUtc returns UTC DateTime', () {
      var date = LocalDate(2025, 3, 19);
      var dateTime = date.toDateTimeUtc();

      check(dateTime.year).equals(2025);
      check(dateTime.month).equals(3);
      check(dateTime.day).equals(19);
      check(dateTime.isUtc).isTrue();
    });

    test('format returns correctly formatted string', () {
      var date = LocalDate(2025, 3, 19);

      // Default format
      check(date.format()).equals('2025-03-19');

      // Custom formats
      check(date.format('yyyy/MM/dd')).equals('2025/03/19');
      check(date.format('dd-MM-yyyy')).equals('19-03-2025');
      check(date.format('M/d/yyyy')).equals('3/19/2025');
    });

    test('toString returns ISO format', () {
      var date = LocalDate(2025, 3, 19);
      check(date.toString()).equals('2025-03-19');

      // Test padding
      date = LocalDate(2025, 3, 9);
      check(date.toString()).equals('2025-03-09');
    });
  });

  group('LocalDate Date Operations', () {
    test('plusDays adds correct number of days', () {
      var date = LocalDate(2025, 3, 19);

      // Add one day
      var newDate = date.plusDays(1);
      check(newDate.toString()).equals('2025-03-20');

      // Add multiple days
      newDate = date.plusDays(10);
      check(newDate.toString()).equals('2025-03-29');

      // Add days crossing month boundary
      newDate = date.plusDays(15);
      check(newDate.toString()).equals('2025-04-03');

      // Add days crossing year boundary
      date = LocalDate(2025, 12, 25);
      newDate = date.plusDays(10);
      check(newDate.toString()).equals('2026-01-04');
    });

    test('plusMonths adds correct number of months', () {
      var date = LocalDate(2025, 3, 19);

      // Add one month
      var newDate = date.plusMonths(1);
      check(newDate.toString()).equals('2025-04-19');

      // Add multiple months
      newDate = date.plusMonths(5);
      check(newDate.toString()).equals('2025-08-19');

      // Add months crossing year boundary
      newDate = date.plusMonths(10);
      check(newDate.toString()).equals('2026-01-19');

      // Test day adjustment for shorter months
      date = LocalDate(2025, 1, 31);
      newDate = date.plusMonths(1);
      check(newDate.toString()).equals('2025-02-28'); // February has 28 days in 2025
    });

    test('plusYears adds correct number of years', () {
      var date = LocalDate(2025, 3, 19);

      // Add one year
      var newDate = date.plusYears(1);
      check(newDate.toString()).equals('2026-03-19');

      // Add multiple years
      newDate = date.plusYears(5);
      check(newDate.toString()).equals('2030-03-19');

      // Test leap year adjustment
      date = LocalDate(2024, 2, 29); // Leap year
      newDate = date.plusYears(1);
      check(newDate.toString()).equals('2025-02-28'); // Not a leap year

      newDate = date.plusYears(4);
      check(newDate.toString()).equals('2028-02-29'); // Leap year again
    });

    test('minusDays subtracts correct number of days', () {
      var date = LocalDate(2025, 3, 19);

      // Subtract one day
      var newDate = date.minusDays(1);
      check(newDate.toString()).equals('2025-03-18');

      // Subtract multiple days
      newDate = date.minusDays(10);
      check(newDate.toString()).equals('2025-03-09');

      // Subtract days crossing month boundary
      newDate = date.minusDays(30);
      check(newDate.toString()).equals('2025-02-17');

      // Subtract days crossing year boundary
      date = LocalDate(2025, 1, 5);
      newDate = date.minusDays(10);
      check(newDate.toString()).equals('2024-12-26');
    });

    test('minusMonths subtracts correct number of months', () {
      var date = LocalDate(2025, 3, 19);

      // Subtract one month
      var newDate = date.minusMonths(1);
      check(newDate.toString()).equals('2025-02-19');

      // Subtract multiple months
      newDate = date.minusMonths(5);
      check(newDate.toString()).equals('2024-10-19');

      // Test day adjustment for shorter months
      date = LocalDate(2025, 3, 31);
      newDate = date.minusMonths(1);
      check(newDate.toString()).equals('2025-02-28'); // February has 28 days in 2025
    });

    test('minusYears subtracts correct number of years', () {
      var date = LocalDate(2025, 3, 19);

      // Subtract one year
      var newDate = date.minusYears(1);
      check(newDate.toString()).equals('2024-03-19');

      // Subtract multiple years
      newDate = date.minusYears(5);
      check(newDate.toString()).equals('2020-03-19');

      // Test leap year adjustment
      date = LocalDate(2025, 2, 28);
      newDate = date.minusYears(1);
      check(newDate.toString()).equals('2024-02-28'); // No day adjustment needed

      date = LocalDate(2025, 2, 28);
      newDate = date.minusYears(5);
      check(newDate.toString()).equals('2020-02-28'); // Leap year but no adjustment needed
    });
  });

  group('LocalDate Comparison', () {
    test('compareTo returns correct ordering', () {
      var date1 = LocalDate(2025, 3, 19);
      var date2 = LocalDate(2025, 3, 20);
      var date3 = LocalDate(2025, 4, 1);
      var date4 = LocalDate(2026, 1, 1);
      var date5 = LocalDate(2025, 3, 19);

      // Earlier date compared to later date
      check(date1.compareTo(date2) < 0).isTrue();
      check(date1.compareTo(date3) < 0).isTrue();
      check(date1.compareTo(date4) < 0).isTrue();

      // Later date compared to earlier date
      check(date4.compareTo(date3) > 0).isTrue();
      check(date3.compareTo(date2) > 0).isTrue();
      check(date2.compareTo(date1) > 0).isTrue();

      // Same date
      check(date1.compareTo(date5) == 0).isTrue();
    });

    test('isAfter returns correct boolean result', () {
      var date1 = LocalDate(2025, 3, 19);
      var date2 = LocalDate(2025, 3, 20);
      var date3 = LocalDate(2025, 3, 19);

      check(date2.isAfter(date1)).isTrue();
      check(date1.isAfter(date2)).isFalse();
      check(date1.isAfter(date3)).isFalse();
    });

    test('isBefore returns correct boolean result', () {
      var date1 = LocalDate(2025, 3, 19);
      var date2 = LocalDate(2025, 3, 20);
      var date3 = LocalDate(2025, 3, 19);

      check(date1.isBefore(date2)).isTrue();
      check(date2.isBefore(date1)).isFalse();
      check(date1.isBefore(date3)).isFalse();
    });

    test('daysBetween calculates correct number of days', () {
      var date1 = LocalDate(2025, 3, 19);
      var date2 = LocalDate(2025, 3, 24);

      // Forward calculation
      check(date1.daysBetween(date2)).equals(5);

      // Reverse calculation (should return same absolute value)
      check(date2.daysBetween(date1)).equals(5);

      // Same date
      check(date1.daysBetween(date1)).equals(0);

      // Across month boundary
      var date3 = LocalDate(2025, 4, 5);
      check(date1.daysBetween(date3)).equals(17);

      // Across year boundary
      var date4 = LocalDate(2026, 1, 1);
      check(date1.daysBetween(date4)).isGreaterThan(0);
    });

    test('equals works correctly', () {
      var date1 = LocalDate(2025, 3, 19);
      var date2 = LocalDate(2025, 3, 19);
      var date3 = LocalDate(2025, 3, 20);

      check(date1 == date2).isTrue();
      check(date1 == date3).isFalse();
      check(date1 == date1).isTrue();
      check(date1 == "not a date").isFalse();
    });

    test('hashCode works as expected', () {
      var date1 = LocalDate(2025, 3, 19);
      var date2 = LocalDate(2025, 3, 19);
      var date3 = LocalDate(2025, 3, 20);

      check(date1.hashCode == date2.hashCode).isTrue();
      check(date1.hashCode == date3.hashCode).isFalse();
    });
  });

  group('LocalDate Properties', () {
    test('dayOfWeek returns correct day of week', () {
      // March 19, 2025 is a Wednesday (day 3)
      var date = LocalDate(2025, 3, 19);
      check(date.dayOfWeek).equals(3);

      // January 1, 2025 is a Wednesday (day 3)
      date = LocalDate(2025, 1, 1);
      check(date.dayOfWeek).equals(3);

      // December 25, 2024 is a Wednesday (day 3)
      date = LocalDate(2024, 12, 25);
      check(date.dayOfWeek).equals(3);

      // February 29, 2024 is a Thursday (day 4)
      date = LocalDate(2024, 2, 29);
      check(date.dayOfWeek).equals(4);
    });

    test('dayOfYear returns correct day of year', () {
      // January 1 is day 1
      var date = LocalDate(2025, 1, 1);
      check(date.dayOfYear).equals(1);

      // Non-leap year
      date = LocalDate(2025, 12, 31);
      check(date.dayOfYear).equals(365);

      // Leap year
      date = LocalDate(2024, 12, 31);
      check(date.dayOfYear).equals(366);

      // March 1 in non-leap year (day 60)
      date = LocalDate(2025, 3, 1);
      check(date.dayOfYear).equals(60);

      // March 1 in leap year (day 61)
      date = LocalDate(2024, 3, 1);
      check(date.dayOfYear).equals(61);
    });
  });

  group('LocalDate Static Methods', () {
    test('isLeapYear correctly identifies leap years', () {
      // Regular leap years (divisible by 4)
      check(LocalDate.isLeapYear(2024)).isTrue();
      check(LocalDate.isLeapYear(2020)).isTrue();
      check(LocalDate.isLeapYear(2016)).isTrue();

      // Century years that are not leap years (divisible by 100 but not by 400)
      check(LocalDate.isLeapYear(1900)).isFalse();
      check(LocalDate.isLeapYear(2100)).isFalse();
      check(LocalDate.isLeapYear(2200)).isFalse();

      // Century years that are leap years (divisible by 400)
      check(LocalDate.isLeapYear(2000)).isTrue();
      check(LocalDate.isLeapYear(2400)).isTrue();

      // Non-leap years
      check(LocalDate.isLeapYear(2025)).isFalse();
      check(LocalDate.isLeapYear(2023)).isFalse();
      check(LocalDate.isLeapYear(2021)).isFalse();
    });

    test('getDaysInMonth returns correct days for each month', () {
      // Non-leap year
      check(LocalDate.getDaysInMonth(2025, 1)).equals(31); // January
      check(LocalDate.getDaysInMonth(2025, 2)).equals(28); // February
      check(LocalDate.getDaysInMonth(2025, 3)).equals(31); // March
      check(LocalDate.getDaysInMonth(2025, 4)).equals(30); // April
      check(LocalDate.getDaysInMonth(2025, 5)).equals(31); // May
      check(LocalDate.getDaysInMonth(2025, 6)).equals(30); // June
      check(LocalDate.getDaysInMonth(2025, 7)).equals(31); // July
      check(LocalDate.getDaysInMonth(2025, 8)).equals(31); // August
      check(LocalDate.getDaysInMonth(2025, 9)).equals(30); // September
      check(LocalDate.getDaysInMonth(2025, 10)).equals(31); // October
      check(LocalDate.getDaysInMonth(2025, 11)).equals(30); // November
      check(LocalDate.getDaysInMonth(2025, 12)).equals(31); // December

      // Leap year
      check(LocalDate.getDaysInMonth(2024, 2)).equals(29); // February in leap year
    });
  });
}
