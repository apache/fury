---
title: Type Mapping of Xlang Serialization
sidebar_position: 3
id: xlang_type_mapping
---

Note:

- For type definition, see [Type Systems in Spec](../specification/xlang_serialization_spec.md#type-systems)
- `int16_t[x]/vector<~>` indicates `int16_t[x]/vector<int16_t>`
- The cross-language serialization is not stable, use it carefully.

# Type Mapping

| Type               | Type ID | Java            | Python               | Javascript      | C++                            | Golang           | Rust             |
|--------------------|---------|-----------------|----------------------|-----------------|--------------------------------|------------------|------------------|
| bool               | 1       | bool/Boolean    | bool                 | Boolean         | bool                           | bool             | bool             |
| int8               | 2       | byte/Byte       | int/pyfury.Int8      | Type.int8()     | int8_t                         | int8             | i8               |
| int16              | 3       | short/Short     | int/pyfury.Int16     | Type.int16()    | int16_t                        | int16            | i6               |
| int32              | 4       | int/Integer     | int/pyfury.Int32     | Type.int32()    | int32_t                        | int32            | i32              |
| var_int32          | 5       | int/Integer     | int/pyfury.VarInt32  | Type.varint32() | fury::varint32_t               | fury.varint32    | fury::varint32   |
| int64              | 6       | long/Long       | int/pyfury.Int64     | Type.int64()    | int64_t                        | int64            | i64              |
| var_int64          | 7       | long/Long       | int/pyfury.VarInt64  | Type.varint64() | fury::varint64_t               | fury.varint64    | fury::varint64   |
| sli_int64          | 8       | long/Long       | int/pyfury.SliInt64  | Type.sliint64() | fury::sliint64_t               | fury.sliint64    | fury::sliint64   |
| float16            | 9       | float/Float     | float/pyfury.Float16 | Type.float16()  | fury::float16_t                | fury.float16     | fury::f16        |
| float32            | 9       | float/Float     | float/pyfury.Float32 | Type.float32()  | float                          | float32          | f32              |
| float64            | 9       | double/Double   | float/pyfury.Float64 | Type.float64()  | double                         | float64          | f64              |
| string             | 9       | String          | str                  | String          | string                         | string           | String/str       |
| enum               | 9       | Enum subclasses | enum subclasses      | /               | enum                           | /                | enum             |
| list               | 9       | List/Collection | list/tuple           | array           | vector                         | slice            | Vec              |
| set                | 9       | Set             | set                  | /               | set                            | fury.Set         | Set              |
| map                | 9       | Map             | dict                 | Map             | unordered_map                  | map              | HashMap          |
| duration           | 9       | Duration        | timedelta            | Number          | duration                       | Duration         | Duration         |
| timestamp          | 9       | Instant         | datetime             | Number          | std::chrono::nanoseconds       | Time             | DateTime         |
| decimal            | 9       | BigDecimal      | Decimal              | bigint          | /                              | /                | /                |
| array              | 9       | array           | np.ndarray           | /               | /                              | array/slice      | Vec              |
| bool_array         | 9       | bool[]          | ndarray(np.bool_)    | /               | `bool[x]`                      | `[x]bool/[]~`    | `Vec<bool>`      |
| int16_array        | 9       | short[]         | ndarray(int16)       | /               | `int16_t[x]/vector<~>`         | `[x]int16/[]~`   | `Vec<i16>`       |
| int32_array        | 9       | int[]           | ndarray(int32)       | /               | `int32_t[x]/vector<~>`         | `[x]int32/[]~`   | `Vec<i32>`       |
| int64_array        | 9       | long[]          | ndarray(int64)       | /               | `int64_t[x]/vector<~>`         | `[x]int64/[]~`   | `Vec<i64>`       |
| float16_array      | 9       | float[]         | ndarray(float16)     | /               | `fury::float16_t[x]/vector<~>` | `[x]float16/[]~` | `Vec<fury::f16>` |
| float32_array      | 9       | float[]         | ndarray(float32)     | /               | `float[x]/vector<~>`           | `[x]float32/[]~` | `Vec<f32>`       |
| float64_array      | 9       | float[]         | ndarray(float64)     | /               | `double[x]/vector<~>`          | `[x]float64/[]~` | `Vec<f64>`       |
| tensor             | 9       | /               | /                    | /               | /                              | /                | /                |
| sparse tensor      | 9       | /               | /                    | /               | /                              | /                | /                |
| arrow record batch | 9       | /               | /                    | /               | /                              | /                | /                |
| arrow table        | 9       | /               | /                    | /               | /                              | /                | /                |

# Type annotation(not implemented)

Due to differences between type systems of languages, those types can't be mapped one-to-one between languages. Users
can provide meta hints for fields of a type, or for the whole type. Such information can be provided in other languages
too:

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

