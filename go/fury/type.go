// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

package fury

import (
	"fmt"
	"hash/fnv"
	"reflect"
	"regexp"
	"strconv"
	"strings"
	"time"
)

type TypeId = int16

const (
	// NA A NullFlag type having no physical storage
	NA TypeId = iota // NA = 0
	// BOOL Boolean as 1 bit LSB bit-packed ordering
	BOOL
	// UINT8 Unsigned 8-bit little-endian integer
	UINT8
	// INT8 Signed 8-bit little-endian integer
	INT8
	// UINT16 Unsigned 16-bit little-endian integer
	UINT16
	// INT16 Signed 16-bit little-endian integer
	INT16
	// UINT32 Unsigned 32-bit little-endian integer
	UINT32
	// INT32 Signed 32-bit little-endian integer
	INT32
	// UINT64 Unsigned 64-bit little-endian integer
	UINT64
	// INT64 Signed 64-bit little-endian integer
	INT64
	// HALF_FLOAT 2-byte floating point value
	HALF_FLOAT
	// FLOAT 4-byte floating point value
	FLOAT
	// DOUBLE 8-byte floating point value
	DOUBLE
	// STRING UTF8 variable-length string as List<Char>
	STRING
	// BINARY Variable-length bytes (no guarantee of UTF8-ness)
	BINARY
	// FIXED_SIZE_BINARY Fixed-size binary. Each value occupies the same number of bytes
	FIXED_SIZE_BINARY
	// DATE32 int32_t days since the UNIX epoch
	DATE32
	// DATE64 int64_t milliseconds since the UNIX epoch
	DATE64
	// TIMESTAMP Exact timestamp encoded with int64 since UNIX epoch
	// Default unit millisecond
	TIMESTAMP
	// TIME32 Time as signed 32-bit integer representing either seconds or
	// milliseconds since midnight
	TIME32
	// TIME64 Time as signed 64-bit integer representing either microseconds or
	// nanoseconds since midnight
	TIME64
	// INTERVAL_MONTHS YEAR_MONTH interval in SQL style
	INTERVAL_MONTHS
	// INTERVAL_DAY_TIME DAY_TIME interval in SQL style
	INTERVAL_DAY_TIME
	// DECIMAL128 Precision- and scale-based decimal type with 128 bits.
	DECIMAL128
	// DECIMAL256 Precision- and scale-based decimal type with 256 bits.
	DECIMAL256
	// LIST A list of some logical data type
	LIST
	// STRUCT Struct of logical types
	STRUCT
	// SPARSE_UNION Sparse unions of logical types
	SPARSE_UNION
	// DENSE_UNION Dense unions of logical types
	DENSE_UNION
	// DICTIONARY Dictionary-encoded type also called "categorical" or "factor"
	// in other programming languages. Holds the dictionary value
	// type but not the dictionary itself which is part of the
	// ArrayData struct
	DICTIONARY
	// MAP Map a repeated struct logical type
	MAP
	// EXTENSION Custom data type implemented by user
	EXTENSION
	// FIXED_SIZE_LIST Fixed size list of some logical type
	FIXED_SIZE_LIST
	// DURATION Measure of elapsed time in either seconds milliseconds microseconds
	// or nanoseconds.
	DURATION
	// LARGE_STRING Like STRING but with 64-bit offsets
	LARGE_STRING
	// LARGE_BINARY Like BINARY but with 64-bit offsets
	LARGE_BINARY
	// LARGE_LIST Like LIST but with 64-bit offsets
	LARGE_LIST
	// MAX_ID Leave this at the end
	MAX_ID
	DECIMAL = DECIMAL128

	// Fury added type for cross-language serialization.
	// FURY_TYPE_TAG for type idendified by the tag
	FURY_TYPE_TAG               = 256
	FURY_SET                    = 257
	FURY_PRIMITIVE_BOOL_ARRAY   = 258
	FURY_PRIMITIVE_SHORT_ARRAY  = 259
	FURY_PRIMITIVE_INT_ARRAY    = 260
	FURY_PRIMITIVE_LONG_ARRAY   = 261
	FURY_PRIMITIVE_FLOAT_ARRAY  = 262
	FURY_PRIMITIVE_DOUBLE_ARRAY = 263
	FURY_STRING_ARRAY           = 264
	FURY_SERIALIZED_OBJECT      = 265
	FURY_BUFFER                 = 266
	FURY_ARROW_RECORD_BATCH     = 267
	FURY_ARROW_TABLE            = 268
)

const (
	NotSupportCrossLanguage = 0
	useStringValue          = 0
	useStringId             = 1
)

var (
	interfaceType = reflect.TypeOf((*interface{})(nil)).Elem()
	stringType    = reflect.TypeOf((*string)(nil)).Elem()
	// Make compilation support tinygo
	stringPtrType = reflect.TypeOf((*string)(nil))
	//stringPtrType      = reflect.TypeOf((**string)(nil)).Elem()
	stringSliceType    = reflect.TypeOf((*[]string)(nil)).Elem()
	byteSliceType      = reflect.TypeOf((*[]byte)(nil)).Elem()
	boolSliceType      = reflect.TypeOf((*[]bool)(nil)).Elem()
	int16SliceType     = reflect.TypeOf((*[]int16)(nil)).Elem()
	int32SliceType     = reflect.TypeOf((*[]int32)(nil)).Elem()
	int64SliceType     = reflect.TypeOf((*[]int64)(nil)).Elem()
	float32SliceType   = reflect.TypeOf((*[]float32)(nil)).Elem()
	float64SliceType   = reflect.TypeOf((*[]float64)(nil)).Elem()
	interfaceSliceType = reflect.TypeOf((*[]interface{})(nil)).Elem()
	interfaceMapType   = reflect.TypeOf((*map[interface{}]interface{})(nil)).Elem()
	boolType           = reflect.TypeOf((*bool)(nil)).Elem()
	byteType           = reflect.TypeOf((*byte)(nil)).Elem()
	int8Type           = reflect.TypeOf((*int8)(nil)).Elem()
	int16Type          = reflect.TypeOf((*int16)(nil)).Elem()
	int32Type          = reflect.TypeOf((*int32)(nil)).Elem()
	int64Type          = reflect.TypeOf((*int64)(nil)).Elem()
	intType            = reflect.TypeOf((*int)(nil)).Elem()
	float32Type        = reflect.TypeOf((*float32)(nil)).Elem()
	float64Type        = reflect.TypeOf((*float64)(nil)).Elem()
	dateType           = reflect.TypeOf((*Date)(nil)).Elem()
	timestampType      = reflect.TypeOf((*time.Time)(nil)).Elem()
	genericSetType     = reflect.TypeOf((*GenericSet)(nil)).Elem()
)

type typeResolver struct {
	typeTagToSerializers map[string]Serializer
	typeToSerializers    map[reflect.Type]Serializer
	typeToTypeInfo       map[reflect.Type]string
	typeToTypeTag        map[reflect.Type]string
	typeInfoToType       map[string]reflect.Type
	typeIdToType         map[int16]reflect.Type
	dynamicStringToId    map[string]int16
	dynamicIdToString    map[int16]string
	dynamicStringId      int16
}

func newTypeResolver() *typeResolver {
	r := &typeResolver{
		typeTagToSerializers: map[string]Serializer{},
		typeToSerializers:    map[reflect.Type]Serializer{},
		typeIdToType:         map[int16]reflect.Type{},
		typeToTypeInfo:       map[reflect.Type]string{},
		typeInfoToType:       map[string]reflect.Type{},
		dynamicStringToId:    map[string]int16{},
		dynamicIdToString:    map[int16]string{},
	}
	// base type info for encode/decode types.
	// composite types info will be constructed dynamically.
	for _, t := range []reflect.Type{
		boolType,
		byteType,
		int8Type,
		int16Type,
		int32Type,
		intType,
		int64Type,
		float32Type,
		float64Type,
		stringType,
		dateType,
		timestampType,
		interfaceType,
		genericSetType, // FIXME set should be a generic type
	} {
		r.typeInfoToType[t.String()] = t
		r.typeToTypeInfo[t] = t.String()
	}
	r.initialize()
	return r
}

func (r *typeResolver) initialize() {
	serializers := []struct {
		reflect.Type
		Serializer
	}{{stringType, stringSerializer{}},
		{stringPtrType, ptrToStringSerializer{}},
		{stringSliceType, stringSliceSerializer{}},
		{byteSliceType, byteSliceSerializer{}},
		{boolSliceType, boolSliceSerializer{}},
		{int16SliceType, int16SliceSerializer{}},
		{int32SliceType, int32SliceSerializer{}},
		{int64SliceType, int64SliceSerializer{}},
		{float32SliceType, float32SliceSerializer{}},
		{float64SliceType, float64SliceSerializer{}},
		{interfaceSliceType, sliceSerializer{}},
		{interfaceMapType, mapSerializer{}},
		{boolType, boolSerializer{}},
		{byteType, byteSerializer{}},
		{int8Type, int8Serializer{}},
		{int16Type, int16Serializer{}},
		{int32Type, int32Serializer{}},
		{int64Type, int64Serializer{}},
		{intType, intSerializer{}},
		{float32Type, float32Serializer{}},
		{float64Type, float64Serializer{}},
		{dateType, dateSerializer{}},
		{timestampType, timeSerializer{}},
		{genericSetType, setSerializer{}},
	}
	for _, elem := range serializers {
		if err := r.RegisterSerializer(elem.Type, elem.Serializer); err != nil {
			panic(fmt.Errorf("impossible error: %s", err))
		}
	}
}

func (r *typeResolver) RegisterSerializer(type_ reflect.Type, s Serializer) error {
	if prev, ok := r.typeToSerializers[type_]; ok {
		return fmt.Errorf("type %s already has a serializer %s registered", type_, prev)
	}
	r.typeToSerializers[type_] = s
	typeId := s.TypeId()
	if typeId != FURY_TYPE_TAG {
		if typeId > NotSupportCrossLanguage {
			if _, ok := r.typeIdToType[typeId]; ok {
				return fmt.Errorf("type %s with id %d has been registered", type_, typeId)
			}
			r.typeIdToType[typeId] = type_
		}
	}
	return nil
}

func (r *typeResolver) RegisterTypeTag(type_ reflect.Type, tag string) error {
	if prev, ok := r.typeToSerializers[type_]; ok {
		return fmt.Errorf("type %s already has a serializer %s registered", type_, prev)
	}
	serializer := &structSerializer{type_: type_, typeTag: tag}
	r.typeToSerializers[type_] = serializer
	// multiple struct with same name defined inside function will have same `type_.String()`, but they are
	// different types. so we use tag to encode type info.
	// tagged type encode as `@$tag`/`*@$tag`.
	r.typeToTypeInfo[type_] = "@" + tag
	r.typeInfoToType["@"+tag] = type_

	ptrType := reflect.PtrTo(type_)
	ptrSerializer := &ptrToStructSerializer{structSerializer: *serializer, type_: ptrType}
	r.typeToSerializers[ptrType] = ptrSerializer
	// use `ptrToStructSerializer` as default deserializer when deserializing data from other languages.
	r.typeTagToSerializers[tag] = ptrSerializer
	r.typeToTypeInfo[ptrType] = "*@" + tag
	r.typeInfoToType["*@"+tag] = ptrType
	return nil
}

func (r *typeResolver) RegisterExt(extId int16, type_ reflect.Type) error {
	// Registering type is necessary, otherwise we may don't have the symbols of corresponding type when deserializing.
	panic("not supported")
}

func (r *typeResolver) getSerializerByType(type_ reflect.Type) (Serializer, error) {
	if serializer, ok := r.typeToSerializers[type_]; !ok {
		if serializer, err := r.createSerializer(type_); err != nil {
			return nil, err
		} else {
			r.typeToSerializers[type_] = serializer
			return serializer, nil
		}
	} else {
		return serializer, nil
	}
}

func (r *typeResolver) getSerializerByTypeTag(typeTag string) (Serializer, error) {
	if serializer, ok := r.typeTagToSerializers[typeTag]; !ok {
		return nil, fmt.Errorf("type %s not supported", typeTag)
	} else {
		return serializer, nil
	}
}

func (r *typeResolver) createSerializer(type_ reflect.Type) (s Serializer, err error) {
	kind := type_.Kind()
	switch kind {
	case reflect.Ptr:
		if elemKind := type_.Elem().Kind(); elemKind == reflect.Ptr || elemKind == reflect.Interface {
			return nil, fmt.Errorf("pointer to pinter/interface are not supported but got type %s", type_)
		}
		valueSerializer, err := r.getSerializerByType(type_.Elem())
		if err != nil {
			return nil, err
		}
		return &ptrToValueSerializer{valueSerializer}, nil
	case reflect.Slice:
		elem := type_.Elem()
		if isDynamicType(elem) {
			return sliceSerializer{}, nil
		} else {
			elemSerializer, err := r.getSerializerByType(type_.Elem())
			if err != nil {
				return nil, err
			}
			return &sliceConcreteValueSerializer{
				type_:          type_,
				elemSerializer: elemSerializer,
				referencable:   nullable(type_.Elem()),
			}, nil
		}
	case reflect.Array:
		elem := type_.Elem()
		if isDynamicType(elem) {
			return arraySerializer{}, nil
		} else {
			elemSerializer, err := r.getSerializerByType(type_.Elem())
			if err != nil {
				return nil, err
			}
			return &arrayConcreteValueSerializer{
				type_:          type_,
				elemSerializer: elemSerializer,
				referencable:   nullable(type_.Elem()),
			}, nil
		}
	case reflect.Map:
		hasKeySerializer, hasValueSerializer := !isDynamicType(type_.Key()), !isDynamicType(type_.Elem())
		if hasKeySerializer || hasValueSerializer {
			var keySerializer, valueSerializer Serializer
			if hasKeySerializer {
				keySerializer, err = r.getSerializerByType(type_.Key())
				if err != nil {
					return nil, err
				}
			}
			if hasValueSerializer {
				valueSerializer, err = r.getSerializerByType(type_.Elem())
				if err != nil {
					return nil, err
				}
			}
			return &mapConcreteKeyValueSerializer{
				type_:             type_,
				keySerializer:     keySerializer,
				valueSerializer:   valueSerializer,
				keyReferencable:   nullable(type_.Key()),
				valueReferencable: nullable(type_.Elem()),
			}, nil
		} else {
			return mapSerializer{}, nil
		}
	}
	return nil, fmt.Errorf("type %s not supported", type_.String())
}

func isDynamicType(type_ reflect.Type) bool {
	return type_.Kind() == reflect.Interface || (type_.Kind() == reflect.Ptr && (type_.Elem().Kind() == reflect.Ptr ||
		type_.Elem().Kind() == reflect.Interface))
}

func (r *typeResolver) writeType(buffer *ByteBuffer, type_ reflect.Type) error {
	typeInfo, ok := r.typeToTypeInfo[type_]
	if !ok {
		if encodeType, err := r.encodeType(type_); err != nil {
			return err
		} else {
			typeInfo = encodeType
			r.typeToTypeInfo[type_] = encodeType
		}
	}
	if err := r.writeMetaString(buffer, typeInfo); err != nil {
		return err
	} else {
		return nil
	}
}

func (r *typeResolver) readType(buffer *ByteBuffer) (reflect.Type, error) {
	metaString, err := r.readMetaString(buffer)
	if err != nil {
		return nil, err
	}
	type_, ok := r.typeInfoToType[metaString]
	if !ok {
		type_, _, err = r.decodeType(metaString)
		if err != nil {
			return nil, err
		} else {
			r.typeInfoToType[metaString] = type_
		}
	}
	return type_, nil
}

func (r *typeResolver) encodeType(type_ reflect.Type) (string, error) {
	if info, ok := r.typeToTypeInfo[type_]; ok {
		return info, nil
	}
	switch kind := type_.Kind(); kind {
	case reflect.Ptr, reflect.Array, reflect.Slice, reflect.Map:
		if elemTypeStr, err := r.encodeType(type_.Elem()); err != nil {
			return "", err
		} else {
			if kind == reflect.Ptr {
				return "*" + elemTypeStr, nil
			} else if kind == reflect.Array {
				return fmt.Sprintf("[%d]", type_.Len()) + elemTypeStr, nil
			} else if kind == reflect.Slice {
				return "[]" + elemTypeStr, nil
			} else if kind == reflect.Map {
				if keyTypeStr, err := r.encodeType(type_.Key()); err != nil {
					return "", err
				} else {
					return fmt.Sprintf("map[%s]%s", keyTypeStr, elemTypeStr), nil
				}
			}
		}
	}
	return type_.String(), nil
}

func (r *typeResolver) decodeType(typeStr string) (reflect.Type, string, error) {
	if type_, ok := r.typeInfoToType[typeStr]; ok {
		return type_, typeStr, nil
	}
	if strings.HasPrefix(typeStr, "*") { // ptr
		subStr := typeStr[len("*"):]
		type_, subStr, err := r.decodeType(subStr)
		if err != nil {
			return nil, "", err
		} else {
			return reflect.PtrTo(type_), "*" + subStr, nil
		}
	} else if strings.HasPrefix(typeStr, "[]") { // slice
		subStr := typeStr[len("[]"):]
		type_, subStr, err := r.decodeType(subStr)
		if err != nil {
			return nil, "", err
		} else {
			return reflect.SliceOf(type_), "[]" + subStr, nil
		}
	} else if strings.HasPrefix(typeStr, "[") { // array
		arrTypeRegex, _ := regexp.Compile(`\[([0-9]+)]`)
		idx := arrTypeRegex.FindStringSubmatchIndex(typeStr)
		if idx == nil {
			return nil, "", fmt.Errorf("unparseable type %s", typeStr)
		}
		lenStr := typeStr[idx[2]:idx[3]]
		if length, err := strconv.Atoi(lenStr); err != nil {
			return nil, "", err
		} else {
			subStr := typeStr[idx[1]:]
			type_, elemStr, err := r.decodeType(subStr)
			if err != nil {
				return nil, "", err
			} else {
				return reflect.ArrayOf(length, type_), typeStr[idx[0]:idx[1]] + elemStr, nil
			}
		}
	} else if strings.HasPrefix(typeStr, "map[") {
		subStr := typeStr[len("map["):]
		keyType, keyStr, err := r.decodeType(subStr)
		if err != nil {
			return nil, "", fmt.Errorf("unparseable map type: %s : %s", typeStr, err)
		} else {
			subStr := typeStr[len("map[")+len(keyStr)+len("]"):]
			valueType, valueStr, err := r.decodeType(subStr)
			if err != nil {
				return nil, "", fmt.Errorf("unparseable map value type: %s : %s", subStr, err)
			} else {
				return reflect.MapOf(keyType, valueType), "map[" + keyStr + "]" + valueStr, nil
			}
		}
	} else {
		if idx := strings.Index(typeStr, "]"); idx >= 0 {
			return r.decodeType(typeStr[:idx])
		}
		if t, ok := r.typeInfoToType[typeStr]; !ok {
			return nil, "", fmt.Errorf("type %s not supported", typeStr)
		} else {
			return t, typeStr, nil
		}
	}
}

func (r *typeResolver) writeTypeTag(buffer *ByteBuffer, typeTag string) error {
	if err := r.writeMetaString(buffer, typeTag); err != nil {
		return err
	} else {
		return nil
	}
}

func (r *typeResolver) readTypeByReadTag(buffer *ByteBuffer) (reflect.Type, error) {
	metaString, err := r.readMetaString(buffer)
	if err != nil {
		return nil, err
	}
	return r.typeTagToSerializers[metaString].(*ptrToStructSerializer).type_, err
}

func (r *typeResolver) readTypeInfo(buffer *ByteBuffer) (string, error) {
	return r.readMetaString(buffer)
}

func (r *typeResolver) getTypeById(id int16) (reflect.Type, error) {
	type_, ok := r.typeIdToType[id]
	if !ok {
		return nil, fmt.Errorf("type of id %d not supported, supported types: %v", id, r.typeIdToType)
	}
	return type_, nil
}

func (r *typeResolver) writeMetaString(buffer *ByteBuffer, str string) error {
	if id, ok := r.dynamicStringToId[str]; !ok {
		dynamicStringId := r.dynamicStringId
		r.dynamicStringId += 1
		r.dynamicStringToId[str] = dynamicStringId
		buffer.WriteVarInt32(int32(len(str) << 1))
		// TODO this hash should be unique, since we don't compare data equality for performance
		h := fnv.New64a()
		if _, err := h.Write([]byte(str)); err != nil {
			return err
		}
		hash := int64(h.Sum64() & 0xffffffffffffff00)
		buffer.WriteInt64(hash)
		if len(str) > MaxInt16 {
			return fmt.Errorf("too long string: %s", str)
		}
		buffer.WriteBinary(unsafeGetBytes(str))
	} else {
		buffer.WriteVarInt32(int32(((id + 1) << 1) | 1))
	}
	return nil
}

func (r *typeResolver) readMetaString(buffer *ByteBuffer) (string, error) {
	header := buffer.ReadVarInt32()
	var length = int(header >> 1)
	if header&0b1 == 0 {
		// TODO support use computed hash
		buffer.ReadInt64()
		str := string(buffer.ReadBinary(length))
		dynamicStringId := r.dynamicStringId
		r.dynamicStringId += 1
		r.dynamicIdToString[dynamicStringId] = str
		return str, nil
	} else {
		return r.dynamicIdToString[int16(length-1)], nil
	}
}

func (r *typeResolver) resetWrite() {
	if r.dynamicStringId > 0 {
		r.dynamicStringToId = map[string]int16{}
		r.dynamicIdToString = map[int16]string{}
		r.dynamicStringId = 0
	}
}

func (r *typeResolver) resetRead() {
	if r.dynamicStringId > 0 {
		r.dynamicStringToId = map[string]int16{}
		r.dynamicIdToString = map[int16]string{}
		r.dynamicStringId = 0
	}
}

func computeStringHash(str string) int32 {
	strBytes := unsafeGetBytes(str)
	var hash int64 = 17
	for _, b := range strBytes {
		hash = hash*31 + int64(b)
		for hash >= MaxInt32 {
			hash = hash / 7
		}
	}
	return int32(hash)
}
