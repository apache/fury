import 'package:fury_core/src/memory/byte_reader.dart';
import 'package:fury_core/src/meta/meta_string_byte.dart';
import 'package:fury_core/src/resolver/impl/meta_str/meta_string_resolver_impl.dart';
import 'package:fury_core/src/resolver/meta_str/ms_handler.dart';

import '../../meta/meta_string.dart';

abstract class MetaStringResolver extends MsHandler{
  const MetaStringResolver();
  static MetaStringResolver get newInst => MetaStringResolverImpl();

  MetaStringBytes readMetaStringBytes(ByteReader reader);
  MetaStringBytes getOrCreateMetaStringBytes(MetaString mstr);
  
  String decodeNamespace(MetaStringBytes msb);
  String decodeTypename(MetaStringBytes msb);
}

// MetaStringBytes readMetaStringBytes(ByteReader reader);
// String readMetaString(ByteReader reader);