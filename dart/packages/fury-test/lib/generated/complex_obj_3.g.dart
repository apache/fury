// GENERATED CODE - DO NOT MODIFY BY HAND

part of '../entity/complex_obj_3.dart';

// **************************************************************************
// FuryObjSpecGenerator
// **************************************************************************

final $ComplexObject3 = ClassSpec(
  ComplexObject3,
  false,
  false,
  [
    FieldSpec(
      'f1',
      TypeSpec(
        List,
        ObjType.LIST,
        false,
        false,
        null,
        const [
          TypeSpec(
            Map,
            ObjType.MAP,
            false,
            false,
            null,
            const [
              TypeSpec(
                int,
                ObjType.INT64,
                false,
                true,
                null,
                const [],
              ),
              TypeSpec(
                Float32,
                ObjType.FLOAT32,
                false,
                true,
                null,
                const [],
              ),
            ],
          ),
        ],
      ),
      true,
      true,
      (Object inst) => (inst as ComplexObject3).f1,
      (Object inst, var v) => (inst as ComplexObject3).f1 =
          List<Map<int, Float32>>.of((v as List).map((v) =>
              Map<int, Float32>.of((v as Map)
                  .map((k, v) => MapEntry((k as int), (v as Float32)))))),
    ),
    FieldSpec(
      'f2',
      TypeSpec(
        HashMap,
        ObjType.MAP,
        false,
        true,
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
            List,
            ObjType.LIST,
            false,
            false,
            null,
            const [
              TypeSpec(
                SplayTreeMap,
                ObjType.MAP,
                false,
                true,
                null,
                const [
                  TypeSpec(
                    int,
                    ObjType.INT64,
                    false,
                    true,
                    null,
                    const [],
                  ),
                  TypeSpec(
                    Float32,
                    ObjType.FLOAT32,
                    false,
                    true,
                    null,
                    const [],
                  ),
                ],
              ),
            ],
          ),
        ],
      ),
      true,
      true,
      (Object inst) => (inst as ComplexObject3).f2,
      (Object inst, var v) => (inst as ComplexObject3).f2 =
          HashMap<String, List<SplayTreeMap<int, Float32>>>.of((v as HashMap)
              .map((k, v) => MapEntry(
                  (k as String),
                  List<SplayTreeMap<int, Float32>>.of((v as List).map((v) =>
                      SplayTreeMap<int, Float32>.of(
                          (v as SplayTreeMap).map((k, v) => MapEntry((k as int), (v as Float32))))))))),
    ),
    FieldSpec(
      'f3',
      TypeSpec(
        LinkedHashMap,
        ObjType.MAP,
        false,
        true,
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
            HashSet,
            ObjType.SET,
            false,
            true,
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
            ],
          ),
        ],
      ),
      true,
      true,
      (Object inst) => (inst as ComplexObject3).f3,
      (Object inst, var v) => (inst as ComplexObject3).f3 =
          LinkedHashMap<String, HashSet<Int8>>.of((v as LinkedHashMap).map(
              (k, v) => MapEntry((k as String),
                  HashSet<Int8>.of((v as HashSet).map((v) => (v as Int8)))))),
    ),
  ],
  null,
  () => ComplexObject3(),
);

mixin _$ComplexObject3Fury implements Furiable {
  @override
  Type get $furyType => ComplexObject3;
}
