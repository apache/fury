import 'package:fury_core/src/const/obj_type.dart';
import 'package:fury_core/src/excep/ser/ser_type_incompatible_excep.dart';

import 'deser/deser_conflict_excep.dart';
import 'deser/deser_invalid_param_excep.dart';
import 'unsupported_type_excep.dart';
import 'ser/ser_range_excep.dart';
import 'register/unregistered_tag_excep.dart';

// 目前想到的均是由于开发者的错误导致的错误，故均继承自Error，而不是Exception， 之后如果有其他类型的错误，再进行修改
abstract class FuryException extends Error{
  // final String _msg;
  // const FuryException(this._msg);
  FuryException();

  void giveExcepMsg(StringBuffer buf){}

  static FuryException derserInvalidParam(
    String invalidParam,
    String validParams,
    [String? where]
  ) => DeserInvalidParamException(invalidParam, validParams, where);


  static FuryException deserConflict(
    String readSetting,
    String nowFurySetting,
    [String? where]
  ) => DeserConflictException(readSetting, nowFurySetting, where);

  static FuryException serRangeExcep(
    ObjType specified,
    num yourValue,
    [String? where]
  ) => SerRangeExcep(specified, yourValue, where);

  static FuryException serTypeIncomp(
    ObjType specified,
    String reason,
    [String? where]
  ) => SerTypeIncompatibleExcep(specified, reason, where);

  static FuryException unregisteredExcep(String tag) => UnregisteredExcep(tag);

  static FuryException deserUnsupportedType(ObjType objType,) =>
    UnsupportedTypeExcep(objType);


}