import 'package:fury/src/deserialize_coordinator.dart';
import 'package:fury/src/meta/spec_wraps/type_spec_wrap.dart';
import 'package:fury/src/resolver/deserialization_ref_resolver.dart';
import 'package:fury/src/resolver/xtype_resolver.dart';
import 'package:fury/src/serializer/fury_header_serializer.dart';
import 'package:fury/src/pack.dart';
import 'package:fury/src/collection/stack.dart';

final class DeserializerPack extends Pack{
   final HeaderBrief header;

   final DeserializeCoordinator furyDeser;

   final DeserializationRefResolver refResolver;
   final XtypeResolver xtypeResolver;

   final Stack<TypeSpecWrap> typeWrapStack;
   
   const DeserializerPack(
      super.structHashResolver,
      super.getTagByDartType,
      this.header,
      this.furyDeser,
      this.refResolver,
      this.xtypeResolver,
      this.typeWrapStack,
   );
}