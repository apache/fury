// GENERATED CODE - DO NOT MODIFY BY HAND

part of 'typed_data_array_example.dart';

// **************************************************************************
// FuryObjSpecGenerator
// **************************************************************************

final $TypedDataArrayExample = ClassSpec(
  TypedDataArrayExample,
  false,
  true,
  [
    FieldSpec(
      'bools',
      TypeSpec(
        BoolList,
        ObjType.BOOL_ARRAY,
        false,
        true,
        null,
        const [],
      ),
      true,
      true,
      (Object inst) => (inst as TypedDataArrayExample).bools,
      (Object inst, var v) =>
          (inst as TypedDataArrayExample).bools = (v as BoolList),
    ),
    FieldSpec(
      'bytes',
      TypeSpec(
        Uint8List,
        ObjType.BINARY,
        false,
        true,
        null,
        const [],
      ),
      true,
      true,
      (Object inst) => (inst as TypedDataArrayExample).bytes,
      (Object inst, var v) =>
          (inst as TypedDataArrayExample).bytes = (v as Uint8List),
    ),
    FieldSpec(
      'nums',
      TypeSpec(
        Int32List,
        ObjType.INT32_ARRAY,
        false,
        true,
        null,
        const [],
      ),
      true,
      true,
      (Object inst) => (inst as TypedDataArrayExample).nums,
      (Object inst, var v) =>
          (inst as TypedDataArrayExample).nums = (v as Int32List),
    ),
  ],
  null,
  () => TypedDataArrayExample(),
);

mixin _$TypedDataArrayExampleFury implements Furiable {
  @override
  Type get $furyType => TypedDataArrayExample;
}
