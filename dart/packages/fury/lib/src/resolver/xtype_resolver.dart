import 'package:fury/src/codegen/entity/struct_hash_pair.dart';
import 'package:fury/src/memory/byte_reader.dart';
import 'package:fury/src/meta/class_info.dart';
import 'package:fury/src/meta/spec_wraps/type_spec_wrap.dart';
import 'package:fury/src/resolver/impl/xtype_resolver_impl.dart';
import 'package:fury/src/serializer/serializer.dart';
import 'package:fury/src/config/fury_config.dart';
import 'package:fury/src/memory/byte_writer.dart';
import 'package:fury/src/meta/specs/custom_type_spec.dart';
import 'package:fury/src/serializer_pack.dart';

abstract base class XtypeResolver{

  const XtypeResolver(FuryConfig conf);

  static XtypeResolver newOne(FuryConfig conf) {
    return XtypeResolverImpl(conf);
  }

  void reg(CustomTypeSpec spec, [String? tag]);

  void registerSerializer(Type type, Serializer ser);

  void setSersForTypeWrap(List<TypeSpecWrap> typeWraps);

  ClassInfo readClassInfo(ByteReader br);

  String getTagByCustomDartType(Type type);

  ClassInfo writeGetClassInfo(ByteWriter bw, Object obj, SerPack pack);

  /*-----For test only------------------------------------------------*/
  StructHashPair getHashPairForTest(
    Type type,
  );
}