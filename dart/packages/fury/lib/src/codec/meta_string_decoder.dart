import 'dart:typed_data';

import 'package:fury/src/codec/meta_string_codecs.dart';
import 'package:fury/src/codec/meta_string_encoding.dart';
import 'package:fury/src/meta/meta_string_byte.dart';

abstract class MetaStringDecoder extends MetaStringCodecs {
  const MetaStringDecoder(super.specialChar1, super.specialChar2);

  String decode(Uint8List data, MetaStrEncoding encoding);
  String decodeMetaString(MetaStringBytes data);
}