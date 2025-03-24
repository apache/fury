// GENERATED CODE - DO NOT MODIFY BY HAND

part of '../entity/some_class.dart';

// **************************************************************************
// FuryObjSpecGenerator
// **************************************************************************

final $SomeClass = ClassSpec(
  SomeClass,
  false,
  false,
  [
    FieldSpec(
      'id',
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
      (Object inst) => (inst as SomeClass).id,
      (Object inst, var v) => (inst as SomeClass).id = (v as int),
    ),
    FieldSpec(
      'map',
      TypeSpec(
        Map,
        ObjType.MAP,
        false,
        false,
        null,
        const [
          TypeSpec(
            String,
            ObjType.STRING,
            false,
            true,
            null,
            const [],
          ),
          TypeSpec(
            double,
            ObjType.FLOAT64,
            false,
            true,
            null,
            const [],
          ),
        ],
      ),
      true,
      true,
      (Object inst) => (inst as SomeClass).map,
      (Object inst, var v) => (inst as SomeClass).map = Map<String, double>.of(
          (v as Map).map((k, v) => MapEntry((k as String), (v as double)))),
    ),
    FieldSpec(
      'name',
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
      (Object inst) => (inst as SomeClass).name,
      (Object inst, var v) => (inst as SomeClass).name = (v as String),
    ),
  ],
  null,
  () => SomeClass.noArgs(),
);

mixin _$SomeClassFury implements Furiable {
  @override
  Type get $furyType => SomeClass;
}
