import 'package:fury/src/collection/stack.dart';
import 'package:fury/src/serialize_coordinator.dart';
import 'package:fury/src/meta/spec_wraps/type_spec_wrap.dart';
import 'package:fury/src/resolver/meta_str/ms_writing_resolver.dart';
import 'package:fury/src/resolver/ref/ser_ref_resolver.dart';
import 'package:fury/src/resolver/xtype_resolver.dart';
import 'package:fury/src/pack.dart';

final class SerPack extends Pack{

  final SerializeCoordinator furySer;
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
}