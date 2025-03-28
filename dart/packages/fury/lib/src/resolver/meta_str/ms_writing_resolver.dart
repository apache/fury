import 'package:fury/src/memory/byte_writer.dart';
import 'package:fury/src/meta/meta_string_byte.dart';
import 'package:fury/src/resolver/impl/meta_str/ms_writing_resolver_impl.dart';
import 'package:fury/src/resolver/meta_str/ms_handler.dart';

abstract class MsWritingResolver extends MsHandler{

  const MsWritingResolver();

  static MsWritingResolver get newInst => MsWritingResolverImpl();

  void writeMsb(ByteWriter bw, MetaStringBytes msb);
}