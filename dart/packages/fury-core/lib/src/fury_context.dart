// CustomSer? getCustomSerByTag(String tag) {
//   return _tag2CustomSer[tag];
// }
//
// String? getTagByDartType(Type type) {
//   CustomSer? ser = _customType2Ser[type];
//   return ser?.tag;
// }
//
// Ser? getSerByObjType(ObjType objType) {
//   return _objId2Ser[objType.id];
// }

// // 没有引用不确定性的type，例如int, double, string, enum等，都必须解析出来，否则会报错
// (List<Ser?> fromSers, List<Ser?> toSers) getFieldsSers(ClassSpec clsSpec){
//   var fields = clsSpec.fields;
//   List<Ser?> fromSers = List.filled(fields.length, null);
//   List<Ser?> toSers = List.filled(fields.length, null);
//   Ser? fromSer;
//   Ser? toSer;
//   for (int i=0;i<fields.length;++i) {
//     var field = fields[i];
//     if (field.includeFromFury) {
//       fromSer = _getSerByFieldType(field.type1.actualFromType, field.type1.type);
//       fromSers[i] = fromSer;
//     }
//     if (field.includeFromFury && field.fromToAlign()){
//       toSers[i] = fromSer;
//     }else{
//       toSer = _getSerByFieldType(field.type1.actualToType, field.type1.type);
//       toSers[i] = toSer;
//     }
//   }
//   return (fromSers, toSers);
// }
//
// Ser? _getSerByFieldType(ObjType objType, Type type) {
//   if (objType.isStructType()) return null;
//   Ser? ser;
//   if (objType == ObjType.NAMED_ENUM) {
//     ser = _customType2Ser[type];
//     if (ser == null) {
//       throw UnregisteredTypeExcep(type);
//     }
//     return ser;
//   }
//   ser = _objId2Ser[objType.id];
//   if (ser == null) {
//     throw UnsupportedTypeExcep(objType);
//   }
//   return ser;
// }
import 'dart:collection';
import 'package:fury_core/src/config/fury_config.dart';
import 'package:fury_core/src/meta/class_info.dart';
import 'package:fury_core/src/ser/ser_pool.dart';
import 'const/dart_type.dart';
import 'const/obj_type.dart';
import 'excep/register/duplicate_spec_registration_excep.dart';
import 'excep/register/duplicate_tag_registration_excep.dart';
import 'ser/ser.dart' show Ser;

class FuryContext {
  // 不可以是static, 因为ClassInfo里有Ser这个字段
  final Iterable<MapEntry<Type,ClassInfo>> _defaultClassInfos =
    DartTypeEnum.values.where(
      (e) => e.objType != null
    ).map(
      (e) => MapEntry(
        e.dartType,
        ClassInfo(e.dartType, e.objType!, null,null,null),
      )
    );

  final FuryConfig conf;
  final Map<String, ClassInfo> tag2ClsInfo; // tag -> ser
  final Map<Type, ClassInfo> type2ClsInfo; // type -> ser
  late final List<ClassInfo?> objTypeId2ClsInfo;

  late final Ser abstractListSer;
  late final Ser abstractMapSer;

  FuryContext(this.conf)
    : tag2ClsInfo = HashMap(),
    type2ClsInfo = HashMap();

  void initForDefaultTypes() {
    type2ClsInfo.addEntries(_defaultClassInfos);
    objTypeId2ClsInfo = SerPool.setSerForDefaultType(type2ClsInfo, conf);
    abstractListSer = objTypeId2ClsInfo[ObjType.LIST.id]!.ser;
    abstractMapSer = objTypeId2ClsInfo[ObjType.MAP.id]!.ser;
  }

  void reg(ClassInfo clsInfo) {
    assert(clsInfo.tag != null);
    ClassInfo? info = type2ClsInfo[clsInfo.dartType];
    /*-----check if the type is already registered -------*/
    if (info!= null) {
      throw DupTypeRegistExcep(info.dartType, info.tag!);
    }
    /*-----check if the tag is already registered -------*/
    info = tag2ClsInfo[clsInfo.tag];
    if (info != null) {
      throw DupTagRegistExcep(clsInfo.tag!, info.dartType, clsInfo.dartType);
    }
    tag2ClsInfo[clsInfo.tag!] = clsInfo;
    type2ClsInfo[clsInfo.dartType] = clsInfo;
  }
}