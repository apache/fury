// GENERATED CODE - DO NOT MODIFY BY HAND

part of '../entity/complex_obj_1.dart';

// **************************************************************************
// FuryObjSpecGenerator
// **************************************************************************

final $ComplexObject1 = ClassSpec(
  ComplexObject1,
  false,
  false,
  [
    FieldSpec(
      'f1',
      TypeSpec(
        Object,
        ObjType.UNKNOWN_YET,
        false,
        false,
        null,
        const [],
      ),
      true,
      true,
      (Object inst) => (inst as ComplexObject1).f1,
      (Object inst, var v) => (inst as ComplexObject1).f1 = (v as Object),
    ),
    FieldSpec(
      'f10',
      TypeSpec(
        double,
        ObjType.FLOAT64,
        false,
        true,
        null,
        const [],
      ),
      true,
      true,
      (Object inst) => (inst as ComplexObject1).f10,
      (Object inst, var v) => (inst as ComplexObject1).f10 = (v as double),
    ),
    FieldSpec(
      'f11',
      TypeSpec(
        Int16List,
        ObjType.INT16_ARRAY,
        false,
        true,
        null,
        const [],
      ),
      true,
      true,
      (Object inst) => (inst as ComplexObject1).f11,
      (Object inst, var v) => (inst as ComplexObject1).f11 = (v as Int16List),
    ),
    FieldSpec(
      'f12',
      TypeSpec(
        List,
        ObjType.LIST,
        false,
        false,
        null,
        const [
          TypeSpec(
            Int16,
            ObjType.INT16,
            false,
            true,
            null,
            const [],
          ),
        ],
      ),
      true,
      true,
      (Object inst) => (inst as ComplexObject1).f12,
      (Object inst, var v) => (inst as ComplexObject1).f12 =
          List<Int16>.of((v as List).map((v) => (v as Int16))),
    ),
    FieldSpec(
      'f2',
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
      (Object inst) => (inst as ComplexObject1).f2,
      (Object inst, var v) => (inst as ComplexObject1).f2 = (v as String),
    ),
    FieldSpec(
      'f3',
      TypeSpec(
        List,
        ObjType.LIST,
        false,
        false,
        null,
        const [
          TypeSpec(
            Object,
            ObjType.UNKNOWN_YET,
            false,
            false,
            null,
            const [],
          ),
        ],
      ),
      true,
      true,
      (Object inst) => (inst as ComplexObject1).f3,
      (Object inst, var v) => (inst as ComplexObject1).f3 =
          List<Object>.of((v as List).map((v) => (v as Object))),
    ),
    FieldSpec(
      'f4',
      TypeSpec(
        Map,
        ObjType.MAP,
        false,
        false,
        null,
        const [
          TypeSpec(
            Int8,
            ObjType.INT8,
            false,
            true,
            null,
            const [],
          ),
          TypeSpec(
            Int32,
            ObjType.INT32,
            false,
            true,
            null,
            const [],
          ),
        ],
      ),
      true,
      true,
      (Object inst) => (inst as ComplexObject1).f4,
      (Object inst, var v) => (inst as ComplexObject1).f4 = Map<Int8, Int32>.of(
          (v as Map).map((k, v) => MapEntry((k as Int8), (v as Int32)))),
    ),
    FieldSpec(
      'f5',
      TypeSpec(
        Int8,
        ObjType.INT8,
        false,
        true,
        null,
        const [],
      ),
      true,
      true,
      (Object inst) => (inst as ComplexObject1).f5,
      (Object inst, var v) => (inst as ComplexObject1).f5 = (v as Int8),
    ),
    FieldSpec(
      'f6',
      TypeSpec(
        Int16,
        ObjType.INT16,
        false,
        true,
        null,
        const [],
      ),
      true,
      true,
      (Object inst) => (inst as ComplexObject1).f6,
      (Object inst, var v) => (inst as ComplexObject1).f6 = (v as Int16),
    ),
    FieldSpec(
      'f7',
      TypeSpec(
        Int32,
        ObjType.INT32,
        false,
        true,
        null,
        const [],
      ),
      true,
      true,
      (Object inst) => (inst as ComplexObject1).f7,
      (Object inst, var v) => (inst as ComplexObject1).f7 = (v as Int32),
    ),
    FieldSpec(
      'f8',
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
      (Object inst) => (inst as ComplexObject1).f8,
      (Object inst, var v) => (inst as ComplexObject1).f8 = (v as int),
    ),
    FieldSpec(
      'f9',
      TypeSpec(
        Float32,
        ObjType.FLOAT32,
        false,
        true,
        null,
        const [],
      ),
      true,
      true,
      (Object inst) => (inst as ComplexObject1).f9,
      (Object inst, var v) => (inst as ComplexObject1).f9 = (v as Float32),
    ),
  ],
  null,
  () => ComplexObject1(),
);

mixin _$ComplexObject1Fury implements Furable {
  @override
  Type get $furyType => ComplexObject1;
}
