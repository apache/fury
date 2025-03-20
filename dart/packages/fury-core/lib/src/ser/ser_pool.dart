// Map<int, Ser> sers = HashMap<int, Ser>();
// // TODO: 没有注册完，现在仅仅作为测试目的
// sers = HashMap<int, Ser>();
//
// const Ser boolSer = BoolSer();
// sers[boolSer.type.id] = boolSer;
//
// const Ser int8Ser = Int8Ser(); // java: byte
// sers[int8Ser.type.id] = int8Ser;
//
// // const Ser uint8Ser = Uint8Ser(); // uint8
// // sers[uint8Ser.type.id] = uint8Ser;
// //
// // const Ser uint16Ser = Uint16Ser(); // uint16
// // sers[uint16Ser.type.id] = uint16Ser;
//
// // no charSer here, because char is not crossLang
//
// const Ser int16Ser = Int16Ser(); // java : short
// sers[int16Ser.type.id] = int16Ser;
//
// const Ser int32Ser = Int32Ser(); // java: int
// sers[int32Ser.type.id] = int32Ser;
//
// const Ser int64Ser = Int64Ser(); // java : long
// sers[int64Ser.type.id] = int64Ser;
//
// const Ser float32Ser = Float32Ser(); // java: float
// sers[float32Ser.type.id] = float32Ser;
//
// const Ser float64Ser = Float64Ser(); // java: double
// sers[float64Ser.type.id] = float64Ser;
//
// I = SerPool._(sers);


// static Ser classSer = ClassSer();
//
// // 虽然使用也能使用Map, 但是List微弱地更快
// // 为了使用const，这里字面上全写出来，
// // 所以请不要责怪这里丑陋
// static final List<Ser?> _sers = [
//   null, // 0 UNKNOWN_YET
//   BoolSer(), // 1 BOOL
//   Int8Ser(), // 2 INT8
//   Int16Ser(), // 3 INT16
//   Int32Ser(), // 4 INT32
//   null, // 5 VAR_INT32
//   Int64Ser(), // 6 INT64
//   null, // 7 VAR_INT64
//   null, // 8 SLI_INT64
//   null, // 9 FLOAT16
//   Float32Ser(), // 10 FLOAT32
//   Float64Ser(), // 11 FLOAT64
//   StringSer(), // 12 STRING
//   null, // 13 ENUM
//   EnumSer(), // 14 NAMED_ENUM
//   null, // 15 STRUCT
//   null, // 16 COMPATIBLE_STRUCT
//   classSer, // 17 NAMED_STRUCT
//   null, // 18 NAMED_COMPATIBLE_STRUCT
//   null, // 19 EXT
//   null, // 20 NAMED_EXT
//   null, // 21 LIST
//   null, // 22 SET
//   null, // 23 MAP
//   null, // 24 DURATION
//   null, // 25 TIMESTAMP
//   null, // 26 LOCAL_DATE
//   null, // 27 DECIMAL
//   null, // 28 BINARY
//   null, // 29 ARRAY
//   null, // 30 BOOL_ARRAY
//   null, // 31 INT8_ARRAY
//   null, // 32 INT16_ARRAY
//   null, // 33 INT32_ARRAY
//   null, // 34 INT64_ARRAY
//   null, // 35 FLOAT16_ARRAY
//   null, // 36 FLOAT32_ARRAY
//   null, // 37 FLOAT64_ARRAY
//   null, // 38 ARROW_RECORD_BATCH
//   null, // 39 ARROW_TABLE
// ];
//
// static const List<bool> _growSample = [true, true, true, true, true];
//
// static Ser? getExactSer(ObjType type){
// if (!type.finalType) return null;
// Ser? ser = _sers[type.id];
// return ser;
// }
//
// static void adaptConfig(FuryConfig conf){
// assert(conf.configId >= 0);
// if (conf.configId >= _sers[1]!.writeRef.length){
// // 说明需要扩展
// // growSample放置的初始元素是true
// _growAllSerWriteRefList();
// }
// for (var ser in _sers){
// if (ser == null) continue;
// if (!conf.refTracking){
// ser.writeRef[conf.configId] = false;
// continue;
// }
// if (ser.forceNoRefWrite){
// ser.writeRef[conf.configId] = false;
// continue;
// }
// if (conf.basicTypesRefIgnored && ser.objType.isBasicType){
// ser.writeRef[conf.configId] = false;
// continue;
// }
// if (conf.timeRefIgnored && ser.objType.isTimeType()){
// ser.writeRef[conf.configId] = false;
// continue;
// }
// if (conf.stringRefIgnored && ser.objType == ObjType.STRING){
// ser.writeRef[conf.configId] = false;
// continue;
// }
// // 到这里无需设置，默认就是true
// }
// }
//
// static void _growAllSerWriteRefList(){
// for (var ser in _sers){
// if (ser == null) continue;
// // 注意，无法使用length进行扩展，因为不是nullable元素
// // dart的List无法进行capacity预订, 也没有add(num, val)这种接口
// // 使用add逐个添加可能导致不必要的扩容，只能使用addAll
// ser.writeRef.addAll(_growSample);
// }
// }

// static CustomSer getSerForCustomType(FuryConfig conf, CustomTypeSpec spec, String tag){
//   if (spec is EnumSpec){
//     return EnumSer.cache.getSerWithSpec(conf, spec, tag);
//   }
//   return ClassSer.cache.getSerWithSpec(conf, spec, tag);
// }
//
// static List<Ser?> giveSersByObjType(FuryConfig conf){
//   return [
//     null, // 0 UNKNOWN_YET
//     BoolSer.cache.getSer(conf), // 1 BOOL
//     Int8Ser.cache.getSer(conf), // 2 INT8
//     Int16Ser.cache.getSer(conf), // 3 INT16
//     Int32Ser.cache.getSer(conf), // 4 INT32
//     null, // 5 VAR_INT32
//     Int64Ser.cache.getSer(conf), // 6 INT64
//     null, // 7 VAR_INT64
//     null, // 8 SLI_INT64
//     null, // 9 FLOAT16
//     Float32Ser.cache.getSer(conf), // 10 FLOAT32
//     Float64Ser.cache.getSer(conf), // 11 FLOAT64
//     StringSer.cache.getSer(conf), // 12 STRING
//     null, // 13 ENUM
//     // EnumSer.cache.getSer(conf), // 14 NAMED_ENUM
//     null, // 15 STRUCT
//     null, // 16 COMPATIBLE_STRUCT
//     // ClassSer.cache.getSer(conf), // 17 NAMED_STRUCT
//     null, // 18 NAMED_COMPATIBLE_STRUCT
//     null, // 19 EXT
//     null, // 20 NAMED_EXT
//     null, // 21 LIST
//     null, // 22 SET
//     null, // 23 MAP
//     null, // 24 DURATION
//     TimestampSer.cache.getSer(conf), // 25 TIMESTAMP
//     DateSer.cache.getSer(conf), // 26 LOCAL_DATE
//     null, // 27 DECIMAL
//     null, // 28 BINARY
//     null, // 29 ARRAY
//     null, // 30 BOOL_ARRAY
//     null, // 31 INT8_ARRAY
//     null, // 32 INT16_ARRAY
//     null, // 33 INT32_ARRAY
//     null, // 34 INT64_ARRAY
//     null, // 35 FLOAT16_ARRAY
//     null, // 36 FLOAT32_ARRAY
//     null, // 37 FLOAT64_ARRAY
//     null, // 38 ARROW_RECORD_B
//   ];
// }

import 'dart:collection';
import 'dart:typed_data';
import 'package:collection/collection.dart';
import 'package:fury_core/src/config/fury_config.dart';
import 'package:fury_core/src/meta/class_info.dart';
import 'package:fury_core/src/ser/array/boollist_ser.dart';
import 'package:fury_core/src/ser/array/uint8list_ser.dart';
import 'package:fury_core/src/ser/collection/list/def_list_ser.dart';
import 'package:fury_core/src/ser/collection/map/linked_hash_map_ser.dart';
import 'package:fury_core/src/ser/primitive/bool_ser.dart';
import 'package:fury_core/src/ser/primitive/float32_ser.dart';
import 'package:fury_core/src/ser/primitive/float64_ser.dart';
import 'package:fury_core/src/ser/primitive/int16_ser.dart';
import 'package:fury_core/src/ser/primitive/int32_ser.dart';
import 'package:fury_core/src/ser/primitive/int64_ser.dart';
import 'package:fury_core/src/ser/primitive/int8_ser.dart';
import 'package:fury_core/src/ser/ser.dart';
import 'package:fury_core/src/ser/other_core/string_ser.dart';
import 'package:fury_core/src/ser/time/date_ser.dart';
import 'package:fury_core/src/ser/time/timestamp_ser.dart';
import 'package:fury_core/src/fury_data_type/int16.dart';
import 'package:fury_core/src/fury_data_type/int32.dart';
import 'package:fury_core/src/fury_data_type/int8.dart';
import 'package:fury_core/src/fury_data_type/float32.dart';

import '../const/dart_type.dart';
import '../const/obj_type.dart';
import '../fury_data_type/local_date.dart';
import '../fury_data_type/timestamp.dart';
import 'array/float32list_ser.dart';
import 'array/float64list_ser.dart';
import 'array/int16list_ser.dart';
import 'array/int32list_ser.dart';
import 'array/int64list_ser.dart';
import 'array/int8list_ser.dart';
import 'collection/map/hashmap_ser.dart';
import 'collection/map/splay_tree_map_ser.dart';
import 'collection/set/hash_set_ser.dart';
import 'collection/set/linked_hash_set_ser.dart';
import 'collection/set/splay_tree_set_ser.dart';

class SerPool{

  static List<ClassInfo?> setSerForDefaultType(
    Map<Type, ClassInfo> type2Ser,
    FuryConfig conf,
  ){
    Ser int64Ser = Int64Ser.cache.getSer(conf);
    Ser boolSer = BoolSer.cache.getSer(conf);
    Ser doubleSer = Float64Ser.cache.getSer(conf);
    Ser int8Ser = Int8Ser.cache.getSer(conf);
    Ser int16Ser = Int16Ser.cache.getSer(conf);
    Ser int32Ser = Int32Ser.cache.getSer(conf);
    Ser float32Ser = Float32Ser.cache.getSer(conf);
    Ser stringSer = StringSer.cache.getSer(conf);
    Ser timeStampSer = TimestampSer.cache.getSer(conf);
    Ser dateSer = DateSer.cache.getSer(conf);

    Ser defListSer = DefListSer.cache.getSer(conf);

    Ser linkedMapSer = LinkedHashMapSer.cache.getSer(conf);
    Ser hashMapSer = HashMapSer.cache.getSer(conf);
    Ser splayTreeMapSer = SplayTreeMapSer.cache.getSer(conf);

    Ser linkedHashSetSer = LinkedHashSetSer.cache.getSer(conf);
    Ser hashSetSer = HashSetSer.cache.getSer(conf);
    Ser splayTreeSetSer = SplayTreeSetSer.cache.getSer(conf);

    Ser uint8ListSer = Uint8ListSer.cache.getSer(conf);
    Ser int8ListSer = Int8ListSer.cache.getSer(conf);
    Ser int16ListSer = Int16ListSer.cache.getSer(conf);
    Ser int32ListSer = Int32ListSer.cache.getSer(conf);
    Ser int64ListSer = Int64ListSer.cache.getSer(conf);
    Ser float32ListSer = Float32ListSer.cache.getSer(conf);
    Ser float64ListSer = Float64ListSer.cache.getSer(conf);
    Ser boolListSer = BoolListSer.cache.getSer(conf);

    type2Ser[int]!.ser = int64Ser;
    type2Ser[bool]!.ser = boolSer;
    type2Ser[TimeStamp]!.ser = timeStampSer;
    type2Ser[LocalDate]!.ser = dateSer;
    type2Ser[double]!.ser = doubleSer;
    type2Ser[Int8]!.ser = int8Ser;
    type2Ser[Int16]!.ser = int16Ser;
    type2Ser[Int32]!.ser = int32Ser;
    type2Ser[Float32]!.ser = float32Ser;
    type2Ser[String]!.ser = stringSer;

    type2Ser[List]!.ser = defListSer;

    type2Ser[Map]!.ser = linkedMapSer;
    type2Ser[LinkedHashMap]!.ser = linkedMapSer;
    type2Ser[HashMap]!.ser = hashMapSer;
    type2Ser[SplayTreeMap]!.ser = splayTreeMapSer;

    type2Ser[Set]!.ser = linkedHashSetSer;
    type2Ser[LinkedHashSet]!.ser = linkedHashSetSer;
    type2Ser[HashSet]!.ser = hashSetSer;
    type2Ser[SplayTreeSet]!.ser = splayTreeSetSer;

    type2Ser[Uint8List]!.ser = uint8ListSer;
    type2Ser[Int8List]!.ser = int8ListSer;
    type2Ser[Int16List]!.ser = int16ListSer;
    type2Ser[Int32List]!.ser = int32ListSer;
    type2Ser[Int64List]!.ser = int64ListSer;
    type2Ser[Float32List]!.ser = float32ListSer;
    type2Ser[Float64List]!.ser = float64ListSer;
    type2Ser[BoolList]!.ser = boolListSer;

    List<ClassInfo?> objTypeId2ClsInfo = List<ClassInfo?>.filled(
      ObjType.values.length,
      null,
    );

    List<DartTypeEnum> values = DartTypeEnum.values;
    for (int i = 0; i< values.length; ++i){
      if (!values[i].supported || !values[i].defForObjType){
        continue;
      }
      objTypeId2ClsInfo[values[i].objType!.id] = type2Ser[values[i].dartType];
    }
    return objTypeId2ClsInfo;
  }
}