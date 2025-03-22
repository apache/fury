import 'package:fury_core/fury_core.dart';
import 'package:fury_core/src/excep/deser/deser_range_excep.dart';
import 'package:fury_core/src/ser/custom/custom_ser.dart';

import '../../config/fury_config.dart';
import '../../deser_pack.dart';
import '../../ser_pack.dart';
import '../ser_cache.dart';

final class _EnumSerCache extends SerCache{

  static final Map<Type, EnumSer> _cache = {};

  const _EnumSerCache();

  @override
  EnumSer getSerWithSpec(FuryConfig conf, covariant EnumSpec spec, Type dartType){
    EnumSer? ser = _cache[dartType];
    if (ser != null) {
      return ser;
    }
    // 在furyJava中，EnumSer是不会进行引用跟踪的
    ser = EnumSer(false, spec.values);
    _cache[dartType] = ser;
    return ser;
  }
}

final class EnumSer extends CustomSer<Enum>{

  static const SerCache cache = _EnumSerCache();

  final List<Enum> values;
  EnumSer(bool writeRef, this.values): super(ObjType.NAMED_ENUM, writeRef);

  @override
  Enum read(ByteReader br, int refId, DeserPack pack) {
    int index = br.readVarUint32Small7();
    // furyJava支持deserializeNonexistentEnumValueAsNull， 这里dart不支持，index超范围一定会报错
    if (index < 0 || index >= values.length) {
      throw DeserRangeExcep(index, values);
    }
    return values[index];
  }

  @override
  void write(ByteWriter bw, Enum v, SerPack pack) {
    bw.writeVarUint32Small7(v.index);
  }
}