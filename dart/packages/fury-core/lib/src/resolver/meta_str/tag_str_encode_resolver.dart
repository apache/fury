import 'package:fury_core/src/meta/meta_string.dart';
import 'package:fury_core/src/resolver/impl/meta_str/tag_str_encode_resolver_impl.dart';

abstract class TagStringEncodeResolver{
  static TagStringEncodeResolver get newInst => TagStringEncodeResolverImpl();
  MetaString encodeTn(String tag);
  MetaString encodeNs(String ns);
}