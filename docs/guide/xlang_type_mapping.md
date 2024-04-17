---
title: Type Mapping of Xlang Serialization
sidebar_position: 3
id: xlang_type_mapping
---

Note:

- For type definition, see [Type Systems in Spec](../specification/xlang_serialization_spec.md#type-systems)
- `int16_t[n]/vector<T>` indicates `int16_t[n]/vector<int16_t>`
- The cross-language serialization is not stable, do not use it in your production environment.

# Type Mapping

| Fury Type          | Fury Type ID | Java            | Python               | Javascript      | C++                            | Golang           | Rust             |
|--------------------|--------------|-----------------|----------------------|-----------------|--------------------------------|------------------|------------------|
| bool               | 1            | bool/Boolean    | bool                 | Boolean         | bool                           | bool             | bool             |
| int8               | 2            | byte/Byte       | int/pyfury.Int8      | Type.int8()     | int8_t                         | int8             | i8               |
| int16              | 3            | short/Short     | int/pyfury.Int16     | Type.int16()    | int16_t                        | int16            | i6               |
| int32              | 4            | int/Integer     | int/pyfury.Int32     | Type.int32()    | int32_t                        | int32            | i32              |
| var_int32          | 5            | int/Integer     | int/pyfury.VarInt32  | Type.varint32() | fury::varint32_t               | fury.varint32    | fury::varint32   |
| int64              | 6            | long/Long       | int/pyfury.Int64     | Type.int64()    | int64_t                        | int64            | i64              |
| var_int64          | 7            | long/Long       | int/pyfury.VarInt64  | Type.varint64() | fury::varint64_t               | fury.varint64    | fury::varint64   |
| sli_int64          | 8            | long/Long       | int/pyfury.SliInt64  | Type.sliint64() | fury::sliint64_t               | fury.sliint64    | fury::sliint64   |
| float16            | 9            | float/Float     | float/pyfury.Float16 | Type.float16()  | fury::float16_t                | fury.float16     | fury::f16        |
| float32            | 10           | float/Float     | float/pyfury.Float32 | Type.float32()  | float                          | float32          | f32              |
| float64            | 11           | double/Double   | float/pyfury.Float64 | Type.float64()  | double                         | float64          | f64              |
| string             | 12           | String          | str                  | String          | string                         | string           | String/str       |
| enum               | 13           | Enum subclasses | enum subclasses      | /               | enum                           | /                | enum             |
| list               | 14           | List/Collection | list/tuple           | array           | vector                         | slice            | Vec              |
| set                | 15           | Set             | set                  | /               | set                            | fury.Set         | Set              |
| map                | 16           | Map             | dict                 | Map             | unordered_map                  | map              | HashMap          |
| duration           | 17           | Duration        | timedelta            | Number          | duration                       | Duration         | Duration         |
| timestamp          | 18           | Instant         | datetime             | Number          | std::chrono::nanoseconds       | Time             | DateTime         |
| decimal            | 19           | BigDecimal      | Decimal              | bigint          | /                              | /                | /                |
| binary             | 20           | byte[]          | bytes                | /               | `uint8_t[n]/vector<T>`         | `[n]uint8/[]T`   | `Vec<uint8_t>`   |
| array              | 21           | array           | np.ndarray           | /               | /                              | array/slice      | Vec              |
| bool_array         | 22           | bool[]          | ndarray(np.bool_)    | /               | `bool[n]`                      | `[n]bool/[]T`    | `Vec<bool>`      |
| int8_array         | 23           | byte[]          | ndarray(int8)        | /               | `int8_t[n]/vector<T>`          | `[n]int8/[]T`    | `Vec<i18>`       |
| int16_array        | 24           | short[]         | ndarray(int16)       | /               | `int16_t[n]/vector<T>`         | `[n]int16/[]T`   | `Vec<i16>`       |
| int32_array        | 25           | int[]           | ndarray(int32)       | /               | `int32_t[n]/vector<T>`         | `[n]int32/[]T`   | `Vec<i32>`       |
| int64_array        | 26           | long[]          | ndarray(int64)       | /               | `int64_t[n]/vector<T>`         | `[n]int64/[]T`   | `Vec<i64>`       |
| float16_array      | 27           | float[]         | ndarray(float16)     | /               | `fury::float16_t[n]/vector<T>` | `[n]float16/[]T` | `Vec<fury::f16>` |
| float32_array      | 28           | float[]         | ndarray(float32)     | /               | `float[n]/vector<T>`           | `[n]float32/[]T` | `Vec<f32>`       |
| float64_array      | 29           | double[]        | ndarray(float64)     | /               | `double[n]/vector<T>`          | `[n]float64/[]T` | `Vec<f64>`       |
| tensor             | 30           | /               | /                    | /               | /                              | /                | /                |
| sparse tensor      | 31           | /               | /                    | /               | /                              | /                | /                |
| arrow record batch | 32           | /               | /                    | /               | /                              | /                | /                |
| arrow table        | 33           | /               | /                    | /               | /                              | /                | /                |

# Type info(not implemented currently)

Due to differences between type systems of languages, those types can't be mapped one-to-one between languages.

If the user notices that one type on a language corresponds to multiple types in Fury type systems, for example, `long`
in java has type `int64/varint64/sliint64`, it means the language lacks some types, and the user must provide extra type
info when using Fury.

## Type annotation

If the type is a field of another class, users can provide meta hints for fields of a type, or for the whole type.
Such information can be provided in other languages too:

- java: use annotation.
- cpp: use macro and template.
- golang: use struct tag.
- python: use typehint.
- rust: use macro.

Here is en example:

- Java:
    ```java
    class Foo {
      @Int32Type(varint = true)
      int f1;
      List<@Int32Type(varint = true) Integer> f2;
    }
    ```
- Python:

    ```python
    class Foo:
        f1: Int32Type(varint=True)
        f2: List[Int32Type(varint=True)]
    ```

## Type wrapper

If the type is not a field of a class, the user must wrap this type with a Fury type to pass the extra type info.

For example, suppose Fury Java provide a `VarInt64` type, when a user invoke `fury.serialize(long_value)`, he need to
invoke like `fury.serialize(new VarInt64(long_value))`.
