import 'package:fury_core/src/fury_deser_director.dart';
import 'package:fury_core/src/meta/spec_wraps/type_spec_wrap.dart';
import 'package:fury_core/src/resolver/ref/deser_ref_resolver.dart';
import 'package:fury_core/src/resolver/xtype_resolver.dart';
import 'package:fury_core/src/ser/fury_header_ser.dart';
import 'package:fury_core/src/serial_pack.dart';

import 'collection/stack.dart';

final class DeserPack extends SerialPack{
   final HeaderBrief header;

   final FuryDeserDirector furyDeser;

   final DeserRefResolver refResolver;
   final XtypeResolver xtypeResolver;

   final Stack<TypeSpecWrap> typeWrapStack;
   
   const DeserPack(
      super.structHashResolver,
      super.getTagByDartType,
      this.header,
      this.furyDeser,
      this.refResolver,
      this.xtypeResolver,
      this.typeWrapStack,
   );
}