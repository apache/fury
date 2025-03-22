import 'package:fury_core/src/code_gen/entity/struct_hash_pair.dart';
import 'package:fury_core/src/memory/byte_reader.dart';
import 'package:fury_core/src/meta/class_info.dart';
import 'package:fury_core/src/meta/spec_wraps/type_spec_wrap.dart';
import 'package:fury_core/src/ser/ser.dart' show Ser;
import '../config/fury_config.dart';
import '../memory/byte_writer.dart';
import '../meta/specs/custom_type_spec.dart';
import '../ser_pack.dart';
import 'impl/xtype_resolver_impl.dart';

abstract base class XtypeResolver{

  const XtypeResolver(FuryConfig conf);

  static XtypeResolver newOne(FuryConfig conf) {
    return XtypeResolverImpl(conf);
  }

  void reg(CustomTypeSpec spec, [String? tag]);

  void registerSerializer(Type type, Ser ser);

  void setSersForTypeWrap(List<TypeSpecWrap> typeWraps);

  ClassInfo readClassInfo(ByteReader br);

  String getTagByCustomDartType(Type type);
  // Ser readToGetSer(ByteReader reader, MetaStringResolver msResolver);

  ClassInfo writeGetClassInfo(ByteWriter bw, Object obj, SerPack pack);

  /*-----For test only------------------------------------------------*/
  StructHashPair getHashPairForTest(
    Type type,
  );
}