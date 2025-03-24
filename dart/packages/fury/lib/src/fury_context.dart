import 'dart:collection';
import 'package:fury/src/config/fury_config.dart';
import 'package:fury/src/exception/registration_exception.dart' show DuplicatedTagRegistrationException, DuplicatedTypeRegistrationException;
import 'package:fury/src/meta/class_info.dart';
import 'package:fury/src/serializer/serializer.dart';
import 'package:fury/src/serializer/serializer_pool.dart';
import 'package:fury/src/const/dart_type.dart';
import 'package:fury/src/const/obj_type.dart';

class FuryContext {
  // Cannot be static because ClassInfo contains the Ser field
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

  late final Serializer abstractListSer;
  late final Serializer abstractMapSer;

  FuryContext(this.conf)
    : tag2ClsInfo = HashMap(),
    type2ClsInfo = HashMap();

  void initForDefaultTypes() {
    type2ClsInfo.addEntries(_defaultClassInfos);
    objTypeId2ClsInfo = SerializerPool.setSerForDefaultType(type2ClsInfo, conf);
    abstractListSer = objTypeId2ClsInfo[ObjType.LIST.id]!.ser;
    abstractMapSer = objTypeId2ClsInfo[ObjType.MAP.id]!.ser;
  }

  void reg(ClassInfo clsInfo) {
    assert(clsInfo.tag != null);
    ClassInfo? info = type2ClsInfo[clsInfo.dartType];
    // Check if the type is already registered
    if (info!= null) {
      throw DuplicatedTypeRegistrationException(info.dartType, info.tag!);
    }
    // Check if the tag is already registered
    info = tag2ClsInfo[clsInfo.tag];
    if (info != null) {
      throw DuplicatedTagRegistrationException(
        clsInfo.tag!, 
        info.dartType, 
        clsInfo.dartType,
      );
    }
    tag2ClsInfo[clsInfo.tag!] = clsInfo;
    type2ClsInfo[clsInfo.dartType] = clsInfo;
  }
}
