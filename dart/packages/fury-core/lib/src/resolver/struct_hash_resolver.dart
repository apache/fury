import 'dart:collection';

import 'package:fury_core/src/code_gen/entity/struct_hash_pair.dart';
import 'package:fury_core/src/const/obj_type.dart';
import 'package:fury_core/src/meta/specs/field_spec.dart';
import 'package:fury_core/src/util/string_util.dart';


class StructHashResolver{
  // singleton
  static final StructHashResolver _instance = StructHashResolver._internal();
  static StructHashResolver get inst => _instance;
  StructHashResolver._internal();

  // key: utf8 string objectIdentityHash, value: hash value
  final Map<int, int> _stringObjToHash = HashMap();

  StructHashPair computeHash(List<FieldSpec> fields, String Function(Type type) getTagByType) {
    if (fields.isEmpty) {
      return const StructHashPair(17, 17);
    }
    int hashF = 17;
    int hashT = 17;
    // 这里检查是否align就是看一下是不是includeFromFury和includeToFury一样，并且二者不可能同时为false,
    // 不然静态分析阶段不会保留这个字段，所以实际上那个就是二者都是true
    bool stillAlign = fields[0].includeFromFury == fields[0].includeToFury;

    for (int i = 0; i < fields.length; ++i){
      if (stillAlign){
        // 这里的stillAlign代表着fields[i] 是align的
        if (i < fields.length - 1){
          stillAlign = fields[i+1].includeFromFury == fields[i+1].includeToFury;
        }
        hashF = _computeFieldHash(hashF, fields[i], getTagByType);
        hashT = hashF;
        continue;
      }
      // 这里的stillAlign代表着fields[i] 是unAlign的
      if (fields[i].includeFromFury) hashF = _computeFieldHash(hashF, fields[i], getTagByType);
      if (fields[i].includeToFury) hashT = _computeFieldHash(hashT, fields[i], getTagByType);
    }
    return StructHashPair(hashF, hashT);
  }

  int _computeFieldHash(int hash, FieldSpec field, String Function(Type type) getTagByType) {
    late int id;
    String tag;
    ObjType objType = field.typeSpec.objType;
    switch(objType){
      case ObjType.LIST:
        id = ObjType.LIST.id;
        break;
      case ObjType.MAP:
        id = ObjType.MAP.id;
        break;
      case ObjType.UNKNOWN_YET:
        id = 0;
        break;
      default:
        if (objType.isStructType()){
          tag = getTagByType(field.typeSpec.type);
          int tagObjHashCode = identityHashCode(tag);
          int? hashVal = _stringObjToHash[tagObjHashCode];
          if (hashVal != null) {
            id = hashVal;
          }else{
            id = StringUtil.computeUtf8StringHash(tag);
            _stringObjToHash[tagObjHashCode] = id;
          }
        }else {
          id = objType.id.abs();
        }
    }
    int fieldHash = hash * 31 + id;
    while (fieldHash > 0x7FFFFFFF){
      fieldHash ~/= 7;
    }
    return fieldHash;
  }
}