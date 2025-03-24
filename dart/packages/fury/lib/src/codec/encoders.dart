import 'package:fury/src/codec/impl/meta_string_decoder_impl.dart';
import 'package:fury/src/codec/impl/meta_string_encoder_impl.dart';
import 'package:fury/src/codec/meta_string_encoder.dart';
import 'package:fury/src/codec/meta_string_decoder.dart';

final class Encoders {
  // Here, to use const optimization, we rely on specific implementation classes for performance considerations
  static const MetaStringEncoder genericEncoder = FuryMetaStringEncoder(46, 95); // '.', '_'
  static const MetaStringDecoder genericDecoder = FuryMetaStringDecoder(46, 95); // '.', '_'
  static const MetaStringEncoder packageEncoder = genericEncoder;
  static const MetaStringDecoder packageDecoder = genericDecoder;
  static const MetaStringEncoder typeNameEncoder = FuryMetaStringEncoder(36, 95); // '$', '_'
  static const MetaStringDecoder typeNameDecoder = FuryMetaStringDecoder(36, 95); // '$', '_'
}
