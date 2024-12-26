using System;
using System.Collections.Generic;
using Fury.Serializer;

namespace Fury;

public static class BuiltIns
{
    public static IReadOnlyDictionary<Type, ISerializer> BuiltInTypeToSerializers { get; } =
        new Dictionary<Type, ISerializer>
        {
            [typeof(bool)] = PrimitiveSerializer<bool>.Instance,
            [typeof(sbyte)] = PrimitiveSerializer<sbyte>.Instance,
            [typeof(byte)] = PrimitiveSerializer<byte>.Instance,
            [typeof(short)] = PrimitiveSerializer<short>.Instance,
            [typeof(ushort)] = PrimitiveSerializer<ushort>.Instance,
            [typeof(int)] = PrimitiveSerializer<int>.Instance,
            [typeof(uint)] = PrimitiveSerializer<uint>.Instance,
            [typeof(long)] = PrimitiveSerializer<long>.Instance,
            [typeof(ulong)] = PrimitiveSerializer<ulong>.Instance,
            [typeof(float)] = PrimitiveSerializer<float>.Instance,
            [typeof(double)] = PrimitiveSerializer<double>.Instance,
            [typeof(string)] = StringSerializer.Instance,
            [typeof(bool[])] = PrimitiveArraySerializer<bool>.Instance,
            [typeof(byte[])] = PrimitiveArraySerializer<byte>.Instance,
            [typeof(short[])] = PrimitiveArraySerializer<short>.Instance,
            [typeof(int[])] = PrimitiveArraySerializer<int>.Instance,
            [typeof(long[])] = PrimitiveArraySerializer<long>.Instance,
            [typeof(float[])] = PrimitiveArraySerializer<float>.Instance,
            [typeof(double[])] = PrimitiveArraySerializer<double>.Instance,
            [typeof(string[])] = new ArraySerializer<string>(StringSerializer.Instance)
        };

    public static IReadOnlyDictionary<Type, IDeserializer> BuiltInTypeToDeserializers { get; } =
        new Dictionary<Type, IDeserializer>
        {
            [typeof(bool)] = PrimitiveDeserializer<bool>.Instance,
            [typeof(sbyte)] = PrimitiveDeserializer<sbyte>.Instance,
            [typeof(byte)] = PrimitiveDeserializer<byte>.Instance,
            [typeof(short)] = PrimitiveDeserializer<short>.Instance,
            [typeof(ushort)] = PrimitiveDeserializer<ushort>.Instance,
            [typeof(int)] = PrimitiveDeserializer<int>.Instance,
            [typeof(uint)] = PrimitiveDeserializer<uint>.Instance,
            [typeof(long)] = PrimitiveDeserializer<long>.Instance,
            [typeof(ulong)] = PrimitiveDeserializer<ulong>.Instance,
            [typeof(float)] = PrimitiveDeserializer<float>.Instance,
            [typeof(double)] = PrimitiveDeserializer<double>.Instance,
            [typeof(string)] = StringDeserializer.Instance,
            [typeof(bool[])] = PrimitiveArrayDeserializer<bool>.Instance,
            [typeof(byte[])] = PrimitiveArrayDeserializer<byte>.Instance,
            [typeof(short[])] = PrimitiveArrayDeserializer<short>.Instance,
            [typeof(int[])] = PrimitiveArrayDeserializer<int>.Instance,
            [typeof(long[])] = PrimitiveArrayDeserializer<long>.Instance,
            [typeof(float[])] = PrimitiveArrayDeserializer<float>.Instance,
            [typeof(double[])] = PrimitiveArrayDeserializer<double>.Instance,
            [typeof(string[])] = new ArrayDeserializer<string>(StringDeserializer.Instance)
        };

    public static IReadOnlyDictionary<Type, TypeInfo> BuiltInTypeToTypeInfos { get; } =
        new Dictionary<Type, TypeInfo>
        {
            [typeof(bool)] = new(TypeId.Bool, typeof(bool)),
            [typeof(sbyte)] = new(TypeId.Int8, typeof(sbyte)),
            [typeof(byte)] = new(TypeId.Int8, typeof(byte)),
            [typeof(short)] = new(TypeId.Int16, typeof(short)),
            [typeof(ushort)] = new(TypeId.Int16, typeof(ushort)),
            [typeof(int)] = new(TypeId.Int32, typeof(int)),
            [typeof(uint)] = new(TypeId.Int32, typeof(uint)),
            [typeof(long)] = new(TypeId.Int64, typeof(long)),
            [typeof(ulong)] = new(TypeId.Int64, typeof(ulong)),
            [typeof(float)] = new(TypeId.Float32, typeof(float)),
            [typeof(double)] = new(TypeId.Float64, typeof(double)),
            [typeof(string)] = new(TypeId.String, typeof(string)),
            [typeof(bool[])] = new(TypeId.BoolArray, typeof(bool[])),
            [typeof(byte[])] = new(TypeId.Int8Array, typeof(byte[])),
            [typeof(short[])] = new(TypeId.Int16Array, typeof(short[])),
            [typeof(int[])] = new(TypeId.Int32Array, typeof(int[])),
            [typeof(long[])] = new(TypeId.Int64Array, typeof(long[])),
            [typeof(float[])] = new(TypeId.Float32Array, typeof(float[])),
            [typeof(double[])] = new(TypeId.Float64Array, typeof(double[]))
        };

    public static IReadOnlyList<TypeId> TypeIds { get; } =
        [
            TypeId.Na,
            TypeId.Bool,
            TypeId.Int8,
            TypeId.Int16,
            TypeId.Int32,
            TypeId.VarInt32,
            TypeId.Int64,
            TypeId.VarInt64,
            TypeId.SliInt64,
            TypeId.Float16,
            TypeId.Float32,
            TypeId.Float64,
            TypeId.String,
            TypeId.Enum,
            TypeId.NsEnum,
            TypeId.Struct,
            TypeId.PolymorphicStruct,
            TypeId.CompatibleStruct,
            TypeId.PolymorphicCompatibleStruct,
            TypeId.NsStruct,
            TypeId.NsPolymorphicStruct,
            TypeId.NsCompatibleStruct,
            TypeId.NsPolymorphicCompatibleStruct,
            TypeId.Ext,
            TypeId.PolymorphicExt,
            TypeId.NsExt,
            TypeId.NsPolymorphicExt,
            TypeId.List,
            TypeId.Set,
            TypeId.Map,
            TypeId.Duration,
            TypeId.Timestamp,
            TypeId.LocalDate,
            TypeId.Decimal,
            TypeId.Binary,
            TypeId.Array,
            TypeId.BoolArray,
            TypeId.Int8Array,
            TypeId.Int16Array,
            TypeId.Int32Array,
            TypeId.Int64Array,
            TypeId.Float16Array,
            TypeId.Float32Array,
            TypeId.Float64Array,
            TypeId.ArrowRecordBatch,
            TypeId.ArrowTable
        ];
}
