import 'dart:collection';

import 'package:fury_core/src/codec/encoders.dart';
import 'package:fury_core/src/codec/meta_string_encoder.dart';

import '../../../codec/meta_string_encoding.dart';
import '../../../meta/meta_string.dart';
import '../../meta_str/tag_str_encode_resolver.dart';

final class TagStringEncodeResolverImpl extends TagStringEncodeResolver{

  final MetaStringEncoder _tnEncoder = Encoders.typeNameEncoder;
  final MetaStringEncoder _nsEncoder = Encoders.packageEncoder;
  
  static const List<MetaStrEncoding> _tagAllowedEncodings = [
    MetaStrEncoding.utf8,
    MetaStrEncoding.luds,
    MetaStrEncoding.ftls,
    MetaStrEncoding.atls,
  ];

  static const List<MetaStrEncoding> _nsAllowedEncodings = [
    MetaStrEncoding.utf8,
    MetaStrEncoding.luds,
    MetaStrEncoding.ftls,
    MetaStrEncoding.atls,
  ];


  final Map<String, MetaString> _tnMetaStringCache = HashMap();
  final Map<String, MetaString> _nsMetaStringCache = HashMap();

  @override
  MetaString encodeTn(String tag){
    MetaString? metaString = _tnMetaStringCache[tag];
    if(metaString != null){
      return metaString;
    }
    metaString = _tnEncoder.encodeByAllowedEncodings(tag, _tagAllowedEncodings);
    _tnMetaStringCache[tag] = metaString;
    return metaString;
  }

  @override
  MetaString encodeNs(String ns) {
    MetaString? metaString = _nsMetaStringCache[ns];
    if(metaString != null){
      return metaString;
    }
    metaString = _nsEncoder.encodeByAllowedEncodings(ns, _nsAllowedEncodings);
    _nsMetaStringCache[ns] = metaString;
    return metaString;
  }
}