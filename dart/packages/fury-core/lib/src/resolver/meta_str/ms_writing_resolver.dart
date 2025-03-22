import 'package:fury_core/src/resolver/meta_str/ms_handler.dart';

import '../../memory/byte_writer.dart';
import '../../meta/meta_string_byte.dart';
import '../impl/meta_str/ms_writing_resolver_impl.dart';

abstract class MsWritingResolver extends MsHandler{

  const MsWritingResolver();

  static MsWritingResolver get newInst => MsWritingResolverImpl();

  void writeMsb(ByteWriter bw, MetaStringBytes msb);

  // void resetAllMsb();
}