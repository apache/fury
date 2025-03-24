import 'dart:collection';
import 'package:fury/src/config/fury_config.dart';
import 'package:fury/src/exception/registration_exception.dart' show DuplicatedTagRegistrationException, DuplicatedTypeRegistrationException;
import 'package:fury/src/meta/type_info.dart';
import 'package:fury/src/serializer/serializer.dart';
import 'package:fury/src/serializer/serializer_pool.dart';
import 'package:fury/src/const/dart_type.dart';
import 'package:fury/src/const/obj_type.dart';

class FuryContext {
  // Cannot be static because TypeInfo contains the Ser field
  final Iterable<MapEntry<Type,TypeInfo>> _defaultTypeInfos =
    DartTypeEnum.values.where(
      (e) => e.objType != null
    ).map(
      (e) => MapEntry(
        e.dartType,
        TypeInfo(e.dartType, e.objType!, null,null,null),
      )
    );

  final FuryConfig conf;
  final Map<String, TypeInfo> tag2TypeInfo;
  final Map<Type, TypeInfo> type2TypeInfo;
  late final List<TypeInfo?> objTypeId2TypeInfo;

  late final Serializer abstractListSer;
  late final Serializer abstractMapSer;

  FuryContext(this.conf)
    : tag2TypeInfo = HashMap(),
    type2TypeInfo = HashMap();

  void initForDefaultTypes() {
    type2TypeInfo.addEntries(_defaultTypeInfos);
    objTypeId2TypeInfo = SerializerPool.setSerForDefaultType(type2TypeInfo, conf);
    abstractListSer = objTypeId2TypeInfo[ObjType.LIST.id]!.ser;
    abstractMapSer = objTypeId2TypeInfo[ObjType.MAP.id]!.ser;
  }

  void reg(TypeInfo typeInfo) {
    assert(typeInfo.tag != null);
    TypeInfo? info = type2TypeInfo[typeInfo.dartType];
    // Check if the type is already registered
    if (info!= null) {
      throw DuplicatedTypeRegistrationException(info.dartType, info.tag!);
    }
    // Check if the tag is already registered
    info = tag2TypeInfo[typeInfo.tag];
    if (info != null) {
      throw DuplicatedTagRegistrationException(
        typeInfo.tag!,
        info.dartType, 
        typeInfo.dartType,
      );
    }
    tag2TypeInfo[typeInfo.tag!] = typeInfo;
    type2TypeInfo[typeInfo.dartType] = typeInfo;
  }
}
