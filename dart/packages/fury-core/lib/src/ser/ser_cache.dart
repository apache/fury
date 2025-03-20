import 'package:fury_core/src/config/fury_config.dart';
import 'package:fury_core/src/meta/specs/custom_type_spec.dart';
import 'ser.dart' show Ser;

abstract base class SerCache{

  const SerCache();

  Ser getSer(FuryConfig conf){
    throw UnimplementedError('SerCache does not support getSer');
  }

  Ser getSerWithSpec(FuryConfig conf, CustomTypeSpec spec, Type dartType){
    throw UnimplementedError('SerCache does not support getEnumSerWithSpec');
  }
}