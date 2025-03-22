import 'package:fury_core/src/codec/meta_string_encoder.dart';
import 'package:fury_core/src/codec/meta_string_decoder.dart';
import 'impl/meta_string_decoder_impl.dart';
import 'impl/meta_string_encoder_impl.dart';

final class Encoders {
  // 这里为了使用const优化，依赖了具体的实现类，为了性能的考量
  static const MetaStringEncoder genericEncoder = FuryMetaStringEncoder(46, 95); // '.', '_'
  static const MetaStringDecoder genericDecoder = FuryMetaStringDecoder(46, 95); // '.', '_'
  static const MetaStringEncoder packageEncoder = genericEncoder;
  static const MetaStringDecoder packageDecoder = genericDecoder;
  static const MetaStringEncoder typeNameEncoder = FuryMetaStringEncoder(36, 95); // '$', '_'
  static const MetaStringDecoder typeNameDecoder = FuryMetaStringDecoder(36, 95); // '$', '_'
}