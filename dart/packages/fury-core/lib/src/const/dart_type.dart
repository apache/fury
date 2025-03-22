import 'dart:collection';
import 'dart:typed_data';

import 'package:collection/collection.dart' show BoolList;
import 'package:decimal/decimal.dart';

import '../fury_data_type/float32.dart';
import '../fury_data_type/int16.dart';
import '../fury_data_type/int32.dart';
import '../fury_data_type/int8.dart';
import '../fury_data_type/local_date.dart';
import '../fury_data_type/timestamp.dart';
import 'obj_type.dart';

/// 关于在静态代码分析阶段进行类型筛查，防止不支持的类型被使用
/// 如果用户声明的字段类型是dart 官方类型，但是即使是官方类型也有不支持的，例如Duration and Decimal...
/// 形态代码检查阶段,所有一旦检测出不支持的类型，就会抛出异常
/// 如果用户声明的字段类型是自定义类型，静态代码检查阶段，无法判断是否支持，因为可能用户声明的类型是某个父类(例如Object)
/// 而且实际运行中一直使用的是支持的子类，所以静态代码检查阶段对于这种情况不介入

// TODO: 为了使用find，所有的DartTypeEnum的name都必须是唯一的，并且枚举变量必须按字符串顺序排列，目前没有出现特殊情况
enum DartTypeEnum{
  BOOL(bool,true, 'bool', 'dart', 'core', ObjType.BOOL, true, 'dart:core@bool'),
  INT8(Int8, true, 'Int8', 'package', 'fury_core/src/fury_data_type/int8.dart', ObjType.INT8, true, 'dart:core@Int8'),
  INT16(Int16, true, 'Int16', 'package', 'fury_core/src/fury_data_type/int16.dart', ObjType.INT16, true, 'dart:core@Int16'),
  INT32(Int32, true, 'Int32', 'package', 'fury_core/src/fury_data_type/int32.dart', ObjType.INT32, true, 'dart:core@Int32'),
  INT(int,true, 'int', 'dart', 'core', ObjType.INT64, true, 'dart:core@int'),
  FLOAT32(Float32, true, 'Float32', 'package', 'fury_core/src/fury_data_type/float32.dart', ObjType.FLOAT32, true, 'dart:core@Float32'),
  DOUBLE(double,true, 'double', 'dart', 'core', ObjType.FLOAT64, true, 'dart:core@double'),
  STRING(String,true, 'String', 'dart', 'core', ObjType.STRING, true, 'dart:core@String'),

  LOCALDATE(LocalDate, true, 'LocalDate', 'package', 'fury_core/src/fury_data_type/local_date.dart', ObjType.LOCAL_DATE, true, 'dart:core@LocalDate'),
  TIMESTAMP(TimeStamp, false, 'TimeStamp', 'package', 'fury_core/src/fury_data_type/timestamp.dart', ObjType.TIMESTAMP, true, 'dart:core@DateTime'),

  BOOLLIST(BoolList, true, 'BoolList', 'package', 'collection/src/boollist.dart', ObjType.BOOL_ARRAY, true, 'dart:typed_data@BoolList'),
  UINT8LIST(Uint8List, true, 'Uint8List', 'dart', 'typed_data', ObjType.BINARY, true, 'dart:typed_data@Uint8List'),
  INT8LIST(Int8List, true, 'Int8List', 'dart', 'typed_data', ObjType.INT8_ARRAY, true, 'dart:typed_data@Int8List'),
  INT16LIST(Int16List, true, 'Int16List', 'dart', 'typed_data', ObjType.INT16_ARRAY, true, 'dart:typed_data@Int16List'),
  INT32LIST(Int32List, true, 'Int32List', 'dart', 'typed_data', ObjType.INT32_ARRAY, true, 'dart:typed_data@Int32List'),
  INT64LIST(Int64List, true, 'Int64List', 'dart', 'typed_data', ObjType.INT64_ARRAY, true, 'dart:typed_data@Int64List'),
  FLOAT32LIST(Float32List, true, 'Float32List', 'dart', 'typed_data', ObjType.FLOAT32_ARRAY, true, 'dart:typed_data@Float32List'),
  FLOAT64LIST(Float64List, true, 'Float64List', 'dart', 'typed_data', ObjType.FLOAT64_ARRAY, true, 'dart:typed_data@Float64List'),

  LIST(List,false, 'List', 'dart', 'core', ObjType.LIST, true, 'dart:core@List'),

  MAP(Map,false, 'Map', 'dart', 'core', ObjType.MAP, true, 'dart:core@Map'),
  LINKEDHASHMAP(LinkedHashMap, true, 'LinkedHashMap', 'dart', 'collection', ObjType.MAP, false, 'dart:collection@LinkedHashMap'),
  HASHMAP(HashMap, true, 'HashMap', 'dart', 'collection', ObjType.MAP, false, 'dart:collection@HashMap'),
  SPLAYTREEMAP(SplayTreeMap, true, 'SplayTreeMap', 'dart', 'collection', ObjType.MAP, false, 'dart:collection@SplayTreeMap'),

  SET(Set,false,'Set', 'dart', 'core', ObjType.SET, true,'dart:core@Set'),
  LINKEDHASHSET(LinkedHashSet, true, 'LinkedHashSet', 'dart', 'collection', ObjType.SET, false, 'dart:collection@LinkedHashSet'),
  HASHSET(HashSet, true, 'HashSet', 'dart', 'collection', ObjType.SET, false, 'dart:collection@HashSet'),
  SPLAYTREESET(SplayTreeSet, true, 'SplayTreeSet', 'dart', 'collection', ObjType.SET, false, 'dart:collection@SplayTreeSet'),


  //TODO: 外部库的类，path可能会更新，这里请注意
  DECIMAL(Decimal,true, 'Decimal', 'package', 'decimal/decimal.dart', null, true, 'package:decimal/decimal.dart@Decimal'),
  DURATION(Duration,true, 'Duration', 'dart', 'core', null, true,'dart:core@Duration'),;


  // // num 不可以不写在这里
  // NUM(num, false, 'num', 'dart', 'core', ObjType.UNKNOWN_YET, 'dart:core@num'),
  // /*--------virtual---------------------------------------------------------------------*/
  // FURYDATE(FuryDate, true, '', '', '', ObjType.LOCAL_DATE, ''),
  // FURYINT8(FuryInt8, true, '', '', '', ObjType.INT8, ''),
  // FURYINT16(FuryInt16, true, '', '', '', ObjType.INT16, ''),
  // FURYINT32(FuryInt32, true, '', '', '', ObjType.INT32, ''),
  // FURYFLOAT32(FuryFloat32, true, '', '', '', ObjType.FLOAT32, '');

  final String scheme;
  final String path;
  final String typeName;
  final Type dartType;
  final bool certainForSer;
  final ObjType? objType; // null即是：确认时dart内部类型，也不支持
  final bool defForObjType; // 这个dart类型是否是objType的默认类型
  final String fullSign; // 这个字段是scheme + path + name的拼接，但是为了不要运行时拼接，所以这里指定好

  const DartTypeEnum(
      this.dartType,
      this.certainForSer,
      this.typeName,
      this.scheme,
      this.path,
      this.objType,
      this.defForObjType,
      this.fullSign,
      );

  static final Map<String,DartTypeEnum> _typeName2Enum = {
    for (var e in DartTypeEnum.values) e.typeName: e,
  };

  bool get supported => objType != null;



  @Deprecated('使用find')
  // TODO: 使用这个方法的前提是，所有的DartTypeEnum的name都必须是唯一的，并且枚举变量必须按字符串顺序排列
  /// 返回null表示明确是dart内部类型，也不支持
  /// 返回UNKNOWN_YET表示不确定
  //TODO: 尝试记录dart analyzer的id,以达到数字比较的效果
  static DartTypeEnum? _find_depre(String name, String scheme, String path){
    int l = 0;
    int r = DartTypeEnum.values.length - 1;
    int mid;
    while(l<=r){
      mid = (l+r) ~/ 2;
      int comp = name.compareTo(values[mid].typeName);
      if (comp < 0){
        r = mid -1;
      }else if (comp > 0){
        l = mid + 1;
      }else{
        if (values[mid].scheme == scheme && values[mid].path == path){
          return values[mid];
        }else{
          return null;
        }
      }
    }
    return null;
  }

  static DartTypeEnum? find(String name, String scheme, String path){
    DartTypeEnum? e = _typeName2Enum[name];
    if (e == null) return null;
    if (e.scheme == scheme && e.path == path){
      return e;
    }else{
      return null;
    }
  }
}