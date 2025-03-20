// GENERATED CODE - DO NOT MODIFY BY HAND

part of '../entity/time_obj.dart';

// **************************************************************************
// FuryObjSpecGenerator
// **************************************************************************

final $TimeObj = ClassSpec(
  TimeObj,
  true,
  true,
  [
    FieldSpec(
      'date1',
      TypeSpec(
        LocalDate,
        ObjType.LOCAL_DATE,
        false,
        true,
        null,
        const [],
      ),
      true,
      true,
      (Object inst) => (inst as TimeObj).date1,
      null,
    ),
    FieldSpec(
      'date2',
      TypeSpec(
        LocalDate,
        ObjType.LOCAL_DATE,
        false,
        true,
        null,
        const [],
      ),
      true,
      true,
      (Object inst) => (inst as TimeObj).date2,
      null,
    ),
    FieldSpec(
      'date3',
      TypeSpec(
        LocalDate,
        ObjType.LOCAL_DATE,
        false,
        true,
        null,
        const [],
      ),
      true,
      true,
      (Object inst) => (inst as TimeObj).date3,
      null,
    ),
    FieldSpec(
      'date4',
      TypeSpec(
        LocalDate,
        ObjType.LOCAL_DATE,
        false,
        true,
        null,
        const [],
      ),
      true,
      true,
      (Object inst) => (inst as TimeObj).date4,
      null,
    ),
    FieldSpec(
      'dateTime1',
      TypeSpec(
        TimeStamp,
        ObjType.TIMESTAMP,
        false,
        false,
        null,
        const [],
      ),
      true,
      true,
      (Object inst) => (inst as TimeObj).dateTime1,
      null,
    ),
    FieldSpec(
      'dateTime2',
      TypeSpec(
        TimeStamp,
        ObjType.TIMESTAMP,
        false,
        false,
        null,
        const [],
      ),
      true,
      true,
      (Object inst) => (inst as TimeObj).dateTime2,
      null,
    ),
    FieldSpec(
      'dateTime3',
      TypeSpec(
        TimeStamp,
        ObjType.TIMESTAMP,
        false,
        false,
        null,
        const [],
      ),
      true,
      true,
      (Object inst) => (inst as TimeObj).dateTime3,
      null,
    ),
    FieldSpec(
      'dateTime4',
      TypeSpec(
        TimeStamp,
        ObjType.TIMESTAMP,
        false,
        false,
        null,
        const [],
      ),
      true,
      true,
      (Object inst) => (inst as TimeObj).dateTime4,
      null,
    ),
  ],
  (List<dynamic> objList) => TimeObj(
    (objList[0] as LocalDate),
    (objList[1] as LocalDate),
    (objList[2] as LocalDate),
    (objList[3] as LocalDate),
    (objList[4] as TimeStamp),
    (objList[5] as TimeStamp),
    (objList[6] as TimeStamp),
    (objList[7] as TimeStamp),
  ),
  null,
);

mixin _$TimeObjFury implements Furable {
  @override
  Type get $furyType => TimeObj;
}
