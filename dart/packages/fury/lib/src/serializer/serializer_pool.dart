import 'dart:collection';
import 'dart:typed_data';
import 'package:collection/collection.dart';
import 'package:fury/src/config/fury_config.dart';
import 'package:fury/src/const/dart_type.dart';
import 'package:fury/src/const/obj_type.dart';
import 'package:fury/src/datatype/float32.dart';
import 'package:fury/src/datatype/int16.dart';
import 'package:fury/src/datatype/int32.dart';
import 'package:fury/src/datatype/int8.dart';
import 'package:fury/src/datatype/local_date.dart';
import 'package:fury/src/datatype/timestamp.dart';
import 'package:fury/src/meta/type_info.dart';
import 'package:fury/src/serializer/boollist_serializer.dart';
import 'package:fury/src/serializer/collection/list/def_list_serializer.dart';
import 'package:fury/src/serializer/collection/map/hashmap_serializer.dart';
import 'package:fury/src/serializer/collection/map/linked_hash_map_serializer.dart';
import 'package:fury/src/serializer/collection/map/splay_tree_map_serializer.dart';
import 'package:fury/src/serializer/collection/set/hash_set_serializer.dart';
import 'package:fury/src/serializer/collection/set/linked_hash_set_serializer.dart';
import 'package:fury/src/serializer/collection/set/splay_tree_set_serializer.dart';
import 'package:fury/src/serializer/primitive_type_serializer.dart';
import 'package:fury/src/serializer/string_serializer.dart';
import 'package:fury/src/serializer/serializer.dart';
import 'package:fury/src/serializer/time/date_serializer.dart';
import 'package:fury/src/serializer/time/timestamp_serializer.dart';
import 'package:fury/src/serializer/typed_data_array_serializer.dart';

class SerializerPool{

  static List<TypeInfo?> setSerForDefaultType(
      Map<Type, TypeInfo> type2Ser,
      FuryConfig conf,
      ){

    Serializer linkedMapSer = LinkedHashMapSerializer.cache.getSer(conf);
    Serializer linkedHashSetSer = LinkedHashSetSerializer.cache.getSer(conf);

    type2Ser[int]!.ser = Int64Serializer.cache.getSer(conf);
    type2Ser[bool]!.ser = BoolSerializer.cache.getSer(conf);
    type2Ser[TimeStamp]!.ser = TimestampSerializer.cache.getSer(conf);
    type2Ser[LocalDate]!.ser = DateSerializer.cache.getSer(conf);
    type2Ser[double]!.ser = Float64Serializer.cache.getSer(conf);
    type2Ser[Int8]!.ser = Int8Serializer.cache.getSer(conf);
    type2Ser[Int16]!.ser = Int16Serializer.cache.getSer(conf);
    type2Ser[Int32]!.ser = Int32Serializer.cache.getSer(conf);
    type2Ser[Float32]!.ser = Float32Serializer.cache.getSer(conf);
    type2Ser[String]!.ser = StringSerializer.cache.getSer(conf);

    type2Ser[List]!.ser = DefListSerializer.cache.getSer(conf);

    type2Ser[Map]!.ser = linkedMapSer;
    type2Ser[LinkedHashMap]!.ser = linkedMapSer;
    type2Ser[HashMap]!.ser = HashMapSerializer.cache.getSer(conf);
    type2Ser[SplayTreeMap]!.ser = SplayTreeMapSerializer.cache.getSer(conf);

    type2Ser[Set]!.ser = linkedHashSetSer;
    type2Ser[LinkedHashSet]!.ser = linkedHashSetSer;
    type2Ser[HashSet]!.ser = HashSetSerializer.cache.getSer(conf);
    type2Ser[SplayTreeSet]!.ser = SplayTreeSetSerializer.cache.getSer(conf);

    type2Ser[Uint8List]!.ser = Uint8ListSerializer.cache.getSer(conf);
    type2Ser[Int8List]!.ser = Int8ListSerializer.cache.getSer(conf);
    type2Ser[Int16List]!.ser = Int16ListSerializer.cache.getSer(conf);
    type2Ser[Int32List]!.ser = Int32ListSerializer.cache.getSer(conf);
    type2Ser[Int64List]!.ser = Int64ListSerializer.cache.getSer(conf);
    type2Ser[Float32List]!.ser = Float32ListSerializer.cache.getSer(conf);
    type2Ser[Float64List]!.ser = Float64ListSerializer.cache.getSer(conf);
    type2Ser[BoolList]!.ser = BoolListSerializer.cache.getSer(conf);

    List<TypeInfo?> objTypeId2TypeInfo = List<TypeInfo?>.filled(
      ObjType.values.length,
      null,
    );

    List<DartTypeEnum> values = DartTypeEnum.values;
    for (int i = 0; i< values.length; ++i){
      if (!values[i].supported || !values[i].defForObjType){
        continue;
      }
      objTypeId2TypeInfo[values[i].objType!.id] = type2Ser[values[i].dartType];
    }
    return objTypeId2TypeInfo;
  }
}