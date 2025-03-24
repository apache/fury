// GENERATED CODE - DO NOT MODIFY BY HAND

part of 'example.dart';

// **************************************************************************
// FuryObjSpecGenerator
// **************************************************************************

final $Person = ClassSpec(
  Person,
  false,
  true,
  [
    FieldSpec(
      'age',
      TypeSpec(
        int,
        ObjType.INT64,
        false,
        true,
        null,
        const [],
      ),
      true,
      true,
      (Object inst) => (inst as Person).age,
      null,
    ),
    FieldSpec(
      'dateOfBirth',
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
      (Object inst) => (inst as Person).dateOfBirth,
      null,
    ),
    FieldSpec(
      'firstName',
      TypeSpec(
        String,
        ObjType.STRING,
        false,
        true,
        null,
        const [],
      ),
      true,
      true,
      (Object inst) => (inst as Person).firstName,
      null,
    ),
    FieldSpec(
      'lastName',
      TypeSpec(
        String,
        ObjType.STRING,
        false,
        true,
        null,
        const [],
      ),
      true,
      true,
      (Object inst) => (inst as Person).lastName,
      null,
    ),
  ],
  (List<dynamic> objList) => Person(
    (objList[2] as String),
    (objList[3] as String),
    (objList[0] as int),
    (objList[1] as LocalDate),
  ),
  null,
);

mixin _$PersonFury implements Furiable {
  @override
  Type get $furyType => Person;
}
