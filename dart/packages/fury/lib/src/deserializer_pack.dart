import 'package:fury/src/deserializer_director.dart';
import 'package:fury/src/meta/spec_wraps/type_spec_wrap.dart';
import 'package:fury/src/resolver/ref/deser_ref_resolver.dart';
import 'package:fury/src/resolver/xtype_resolver.dart';
import 'package:fury/src/serializer/fury_header_serializer.dart';
import 'package:fury/src/pack.dart';
import 'package:fury/src/collection/stack.dart';

final class DeserializerPack extends Pack{
   final HeaderBrief header;

   final DeserializeDirector furyDeser;

   final DeserRefResolver refResolver;
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