library;

/** --------------- Export Statements --------------- **/

// Base annotations
export 'src/annotation/fury_key.dart';
export 'src/annotation/fury_class.dart';
export 'src/annotation/fury_enum.dart';
export 'src/annotation/fury_constructor.dart';

// Analysis meta
export 'src/meta/specs/class_spec.dart';
export 'src/meta/specs/field_spec.dart';
export 'src/meta/specs/type_spec.dart';
export 'src/meta/specs/enum_spec.dart';

// Fury implementation
export 'src/fury_impl.dart';

// Constants
export 'src/const/lang.dart';
export 'src/const/obj_type.dart';

// User-related
export 'src/furiable.dart';

// Serialization Components
export 'src/serializer_pack.dart';
export 'src/deserializer_pack.dart';

// External packages
export 'package:decimal/decimal.dart';
export 'package:collection/collection.dart';

// Fury data types
export 'src/datatype/fury_fixed_num.dart';
export 'src/datatype/int8.dart';
export 'src/datatype/int16.dart';
export 'src/datatype/int32.dart';
export 'src/datatype/float32.dart';
export 'src/datatype/local_date.dart';
export 'src/datatype/timestamp.dart';

// Serializers
export 'src/serializer/serializer.dart';

// Memory management
export 'src/memory/byte_reader.dart';
export 'src/memory/byte_writer.dart';
