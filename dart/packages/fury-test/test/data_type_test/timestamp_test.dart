// @Skip()
library;

import 'package:fury_core/fury_core.dart';
import 'package:test/test.dart';
import 'package:checks/checks.dart'; // For more expressive assertions

void main() {
  group('TimeStamp constructors', () {
    test('basic constructor', () {
      final ts = TimeStamp(1000000);
      check(ts.microsecondsSinceEpoch).equals(1000000);
    });

    test('now() constructor creates timestamp close to current time', () {
      final now = DateTime.now();
      final tsNow = TimeStamp.now();
      final diff = (tsNow.microsecondsSinceEpoch - now.microsecondsSinceEpoch).abs();

      // Should be created within a reasonable time window (50ms)
      check(diff).isLessThan(50000);
    });

    test('fromMillisecondsSinceEpoch constructor', () {
      final ts = TimeStamp.fromMillisecondsSinceEpoch(1000);
      check(ts.microsecondsSinceEpoch).equals(1000 * 1000);
      check(ts.toMillisecondsSinceEpoch()).equals(1000);
    });

    test('fromDateTime constructor', () {
      final dt = DateTime(2025, 3, 19, 16, 38, 21);
      final ts = TimeStamp.fromDateTime(dt);
      check(ts.microsecondsSinceEpoch).equals(dt.microsecondsSinceEpoch);
    });

    test('fromDateComponents constructor', () {
      final ts = TimeStamp.fromDateComponents(
          year: 2025, month: 3, day: 19, hour: 16, minute: 38, second: 21);

      final dt = ts.toDateTime();
      check(dt.year).equals(2025);
      check(dt.month).equals(3);
      check(dt.day).equals(19);
      check(dt.hour).equals(16);
      check(dt.minute).equals(38);
      check(dt.second).equals(21);
      check(dt.isUtc).isFalse(); // Should be local time
    });

    test('fromUtcDateComponents constructor', () {
      final ts = TimeStamp.fromUtcDateComponents(
          year: 2025, month: 3, day: 19, hour: 16, minute: 38, second: 21);

      final dt = ts.toUtcDateTime();
      check(dt.year).equals(2025);
      check(dt.month).equals(3);
      check(dt.day).equals(19);
      check(dt.hour).equals(16);
      check(dt.minute).equals(38);
      check(dt.second).equals(21);
      check(dt.isUtc).isTrue(); // Should be UTC
    });
  });

  group('TimeStamp conversion methods', () {
    test('toMillisecondsSinceEpoch', () {
      final ts = TimeStamp(1234567000);
      check(ts.toMillisecondsSinceEpoch()).equals(1234567);
    });

    test('toSecondsSinceEpoch', () {
      final ts = TimeStamp(1234567000000);
      check(ts.toSecondsSinceEpoch()).equals(1234567);
    });

    test('toDateTime and toUtcDateTime', () {
      final ts = TimeStamp(1714490301000000); // 2024-04-30 15:18:21 UTC

      final localDt = ts.toDateTime();
      check(localDt.isUtc).isFalse();
      check(localDt.microsecondsSinceEpoch).equals(1714490301000000);

      final utcDt = ts.toUtcDateTime();
      check(utcDt.isUtc).isTrue();
      check(utcDt.microsecondsSinceEpoch).equals(1714490301000000);
      check(utcDt.year).equals(2024);
      check(utcDt.month).equals(4);
      check(utcDt.day).equals(30);
      check(utcDt.hour).equals(15);
      check(utcDt.minute).equals(18);
      check(utcDt.second).equals(21);
    });
  });

  group('TimeStamp arithmetic', () {
    test('add duration', () {
      final ts = TimeStamp(1000000);
      final newTs = ts.add(Duration(seconds: 10));

      check(newTs.microsecondsSinceEpoch).equals(11000000);
      check(newTs.toSecondsSinceEpoch() - ts.toSecondsSinceEpoch()).equals(10);
    });

    test('subtract duration', () {
      final ts = TimeStamp(11000000);
      final newTs = ts.subtract(Duration(seconds: 10));

      check(newTs.microsecondsSinceEpoch).equals(1000000);
      check(ts.toSecondsSinceEpoch() - newTs.toSecondsSinceEpoch()).equals(10);
    });

    test('difference between timestamps', () {
      final ts1 = TimeStamp(1000000);
      final ts2 = TimeStamp(11000000);

      final diff = ts2.difference(ts1);
      check(diff.inSeconds).equals(10);

      final diff2 = ts1.difference(ts2);
      check(diff2.inSeconds).equals(-10);
    });
  });

  group('TimeStamp comparison', () {
    test('isBefore', () {
      final earlier = TimeStamp(1000000);
      final later = TimeStamp(2000000);

      check(earlier.isBefore(later)).isTrue();
      check(later.isBefore(earlier)).isFalse();
      check(earlier.isBefore(earlier)).isFalse();
    });

    test('isAfter', () {
      final earlier = TimeStamp(1000000);
      final later = TimeStamp(2000000);

      check(later.isAfter(earlier)).isTrue();
      check(earlier.isAfter(later)).isFalse();
      check(earlier.isAfter(earlier)).isFalse();
    });

    test('isAtSameMomentAs', () {
      final ts1 = TimeStamp(1000000);
      final ts2 = TimeStamp(1000000);
      final ts3 = TimeStamp(2000000);

      check(ts1.isAtSameMomentAs(ts2)).isTrue();
      check(ts1.isAtSameMomentAs(ts3)).isFalse();
    });

    test('equality operator', () {
      final ts1 = TimeStamp(1000000);
      final ts2 = TimeStamp(1000000);
      final ts3 = TimeStamp(2000000);

      check(ts1 == ts2).isTrue();
      check(ts1 == ts3).isFalse();
      check(ts1.hashCode == ts2.hashCode).isTrue();
    });
  });

  group('TimeStamp formatting', () {
    test('default format (ISO8601)', () {
      // 2025-03-19 16:38:21 UTC
      final ts = TimeStamp.fromUtcDateComponents(
          year: 2025, month: 3, day: 19, hour: 16, minute: 38, second: 21);

      check(ts.format()).equals('2025-03-19T16:38:21.000Z');
      check(ts.toString()).equals('2025-03-19T16:38:21.000Z');
    });

    test('custom format', () {
      final ts = TimeStamp.fromUtcDateComponents(
          year: 2025, month: 3, day: 19, hour: 16, minute: 38, second: 21);

      check(ts.format(pattern: 'yyyy-MM-dd')).equals('2025-03-19');
      check(ts.format(pattern: 'HH:mm:ss')).equals('16:38:21');
      check(ts.format(pattern: 'yyyy-MM-dd HH:mm:ss')).equals('2025-03-19 16:38:21');
    });
  });

  group('Real-world scenarios', () {
    test('creating timestamp for specific date and calculating difference', () {
      // Current date from example: 2025-03-19 16:38:21
      final currentTime = TimeStamp.fromUtcDateComponents(
          year: 2025, month: 3, day: 19, hour: 16, minute: 38, second: 21);

      // A future date (1 week later)
      final futureTime = TimeStamp.fromUtcDateComponents(
          year: 2025, month: 3, day: 26, hour: 16, minute: 38, second: 21);

      final difference = futureTime.difference(currentTime);
      check(difference.inDays).equals(7);
    });

    test('recording event timestamps and calculating duration', () {
      // Simulate a sequence of events with timestamps
      final eventStart = TimeStamp.fromUtcDateComponents(
          year: 2025, month: 3, day: 19, hour: 16, minute: 0, second: 0);

      final eventEnd = TimeStamp.fromUtcDateComponents(
          year: 2025, month: 3, day: 19, hour: 16, minute: 30, second: 0);

      final duration = eventEnd.difference(eventStart);
      check(duration.inMinutes).equals(30);
    });
  });
}