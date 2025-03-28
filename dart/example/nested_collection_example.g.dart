// GENERATED CODE - DO NOT MODIFY BY HAND

part of 'nested_collection_example.dart';

// **************************************************************************
// FuryObjSpecGenerator
// **************************************************************************

final $NestedObject = ClassSpec(
  NestedObject,
  false,
  false,
  [
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
            double,
            ObjType.FLOAT64,
            false,
            true,
            null,
            const [],
          ),
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
                int,
                ObjType.INT64,
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
      (Object inst) => (inst as NestedObject).map,
      (Object inst, var v) => (inst as NestedObject).map =
          Map<double, Map<String, int>>.of((v as Map).map((k, v) => MapEntry(
              (k as double),
              Map<String, int>.of((v as Map)
                  .map((k, v) => MapEntry((k as String), (v as int))))))),
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
      (Object inst) => (inst as NestedObject).name,
      (Object inst, var v) => (inst as NestedObject).name = (v as String),
    ),
    FieldSpec(
      'names',
      TypeSpec(
        List,
        ObjType.LIST,
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
        ],
      ),
      true,
      true,
      (Object inst) => (inst as NestedObject).names,
      (Object inst, var v) => (inst as NestedObject).names =
          List<String>.of((v as List).map((v) => (v as String))),
    ),
  ],
  null,
  () => NestedObject(),
);

mixin _$NestedObjectFury implements Furiable {
  @override
  Type get $furyType => NestedObject;
}
