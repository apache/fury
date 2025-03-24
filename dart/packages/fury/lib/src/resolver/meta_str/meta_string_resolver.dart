import 'package:fury/src/memory/byte_reader.dart';
import 'package:fury/src/meta/meta_string.dart';
import 'package:fury/src/meta/meta_string_byte.dart';
import 'package:fury/src/resolver/impl/meta_str/meta_string_resolver_impl.dart';
import 'package:fury/src/resolver/meta_str/ms_handler.dart';

abstract class MetaStringResolver extends MsHandler{
  const MetaStringResolver();
  static MetaStringResolver get newInst => MetaStringResolverImpl();

  MetaStringBytes readMetaStringBytes(ByteReader reader);
  MetaStringBytes getOrCreateMetaStringBytes(MetaString mstr);
  
  String decodeNamespace(MetaStringBytes msb);
  String decodeTypename(MetaStringBytes msb);
}