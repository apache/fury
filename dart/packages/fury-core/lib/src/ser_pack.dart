import 'package:fury_core/src/fury_ser_director.dart';
import 'package:fury_core/src/resolver/meta_str/ms_writing_resolver.dart';
import 'package:fury_core/src/resolver/ref/ser_ref_resolver.dart';
import 'package:fury_core/src/resolver/xtype_resolver.dart';
import 'package:fury_core/src/serial_pack.dart';

import 'collection/stack.dart';
import 'meta/spec_wraps/type_spec_wrap.dart';

final class SerPack extends SerialPack{

  final FurySerDirector furySer;
  final XtypeResolver xtypeResolver;
  final SerRefResolver refResolver;
  final SerRefResolver noRefResolver;
  final MsWritingResolver msWritingResolver;
  final Stack<TypeSpecWrap> typeWrapStack;

  const SerPack(
    super.structHashResolver,
    super.getTagByDartType,
    this.furySer,
    this.xtypeResolver,
    this.refResolver,
    this.noRefResolver,
    this.msWritingResolver,
    this.typeWrapStack,
  );

  void resetAndRecycle(){
    // msWritingResolver.resetAllMsb();
  }
}