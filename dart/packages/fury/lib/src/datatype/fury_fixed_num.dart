import 'float16.dart';
import 'float32.dart';
import 'int16.dart';
import 'int32.dart';
import 'int8.dart';

/// Base abstract class for fixed-size numeric types
abstract base class FixedNum implements Comparable<FixedNum>{

  num get value;

  // Factory constructor to create the appropriate type
  static FixedNum from(num value, {String type = 'int32'}) {
    switch (type.toLowerCase()) {
      case 'int8': return Int8(value);
      case 'int16': return Int16(value);
      case 'int32': return Int32(value);
      case 'float16': return Float16(value);
      case 'float32': return Float32(value);
      default: throw ArgumentError('Unknown fixed numeric type: $type');
    }
  }

  @override
  int compareTo(FixedNum other) => value.compareTo(other.value);
}