import 'package:fury/src/config/fury_config.dart';
import 'package:fury/src/meta/specs/custom_type_spec.dart';
import 'package:fury/src/serializer/serializer.dart';

abstract base class SerializerCache{

  const SerializerCache();

  Serializer getSer(FuryConfig conf){
    throw UnimplementedError('SerCache does not support getSer');
  }

  Serializer getSerWithSpec(FuryConfig conf, CustomTypeSpec spec, Type dartType){
    throw UnimplementedError('SerCache does not support getEnumSerWithSpec');
  }
}