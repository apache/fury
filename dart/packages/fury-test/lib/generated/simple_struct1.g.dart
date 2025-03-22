// GENERATED CODE - DO NOT MODIFY BY HAND

part of '../entity/simple_struct1.dart';

// **************************************************************************
// FuryObjSpecGenerator
// **************************************************************************

final $SimpleStruct1 = ClassSpec(
  SimpleStruct1,
  false,
  true,
  [
    FieldSpec(
      'a',
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
      (Object inst) => (inst as SimpleStruct1).a,
      (Object inst, var v) => (inst as SimpleStruct1).a = (v as Int32),
    ),
  ],
  null,
  () => SimpleStruct1(),
);

mixin _$SimpleStruct1Fury implements Furable {
  @override
  Type get $furyType => SimpleStruct1;
}
