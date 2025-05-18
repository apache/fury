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
	"reflect"
)

const (
	CollectionDefaultFlag        = 0b0000
	CollectionTrackingRef        = 0b0001
	CollectionHasNull            = 0b0010
	CollectionNotDeclElementType = 0b0100
	CollectionNotSameType        = 0b1000
)

type sliceSerializer struct{}

func (s sliceSerializer) TypeId() TypeId {
	return LIST
}

func (s sliceSerializer) Write(f *Fury, buf *ByteBuffer, value reflect.Value) error {
	length := value.Len()
	if length == 0 {
		buf.WriteVarUint32(0)
		return nil
	}

	collectFlag, elemTypeInfo := s.writeHeader(f, buf, value)

	if (collectFlag & CollectionNotSameType) == 0 {
		return s.writeSameType(f, buf, value, elemTypeInfo, collectFlag)
	}
	return s.writeDifferentTypes(f, buf, value)
}

func (s sliceSerializer) writeHeader(f *Fury, buf *ByteBuffer, value reflect.Value) (byte, TypeInfo) {
	collectFlag := CollectionDefaultFlag
	var elemTypeInfo TypeInfo
	hasNull := false
	hasDifferentType := false
	elemTypeInfo, _ = f.typeResolver.getTypeInfo(value, true)

	collectFlag |= CollectionNotDeclElementType
	//}

	// 遍历元素检测类型
	for i := 0; i < value.Len(); i++ {
		elem := value.Index(i).Elem()
		if isNull(elem) {
			hasNull = true
			continue
		}

		currentTypeInfo, _ := f.typeResolver.getTypeInfo(elem, true)
		if currentTypeInfo.TypeID != elemTypeInfo.TypeID {
			hasDifferentType = true
		}
	}

	// 设置标志位
	if hasNull {
		collectFlag |= CollectionHasNull
	}
	if hasDifferentType {
		collectFlag |= CollectionNotSameType
	}

	// 引用跟踪
	if f.referenceTracking {
		collectFlag |= CollectionTrackingRef
	}

	// 写入元数据
	buf.WriteVarUint32(uint32(value.Len()))
	buf.WriteInt8(int8(collectFlag))

	// 写入元素类型信息
	if !hasDifferentType {
		buf.WriteVarInt32(elemTypeInfo.TypeID)
	}

	return byte(collectFlag), elemTypeInfo
}

func (s sliceSerializer) writeSameType(f *Fury, buf *ByteBuffer, value reflect.Value, typeInfo TypeInfo, flag byte) error {
	serializer := typeInfo.Serializer
	trackRefs := (flag & CollectionTrackingRef) != 0

	for i := 0; i < value.Len(); i++ {
		elem := value.Index(i).Elem()
		if isNull(elem) {
			buf.WriteInt8(NullFlag)
			continue
		}

		if trackRefs {
			refWritten, err := f.refResolver.WriteRefOrNull(buf, elem)
			if err != nil {
				return err
			}
			if !refWritten {
				if err := serializer.Write(f, buf, elem); err != nil {
					return err
				}
			}
		} else {
			if err := serializer.Write(f, buf, elem); err != nil {
				return err
			}
		}
	}
	return nil
}

func (s sliceSerializer) writeDifferentTypes(f *Fury, buf *ByteBuffer, value reflect.Value) error {
	for i := 0; i < value.Len(); i++ {
		elem := value.Index(i).Elem()
		if isNull(elem) {
			buf.WriteInt8(NullFlag)
			continue
		}
		refWritten, err := f.refResolver.WriteRefOrNull(buf, elem)
		if err != nil {
			return err
		}
		if refWritten {
			continue
		}
		typeInfo, _ := f.typeResolver.getTypeInfo(elem, true)
		buf.WriteVarInt32(typeInfo.TypeID)

		if err := typeInfo.Serializer.Write(f, buf, elem); err != nil {
			return err
		}

	}
	return nil
}

func (s sliceSerializer) Read(f *Fury, buf *ByteBuffer, type_ reflect.Type, value reflect.Value) error {
	length := int(buf.ReadVarUint32())
	if length == 0 {
		value.Set(reflect.MakeSlice(type_, 0, 0))
		return nil
	}

	collectFlag := buf.ReadInt8()
	var elemTypeInfo TypeInfo

	// 读取元素类型
	if (collectFlag & CollectionNotSameType) == 0 {
		typeID := buf.ReadVarInt32()
		elemTypeInfo, _ = f.typeResolver.getTypeInfoById(int16(typeID))
	}

	// 初始化切片
	if value.Cap() < length {
		value.Set(reflect.MakeSlice(type_, length, length))
	} else {
		value.Set(value.Slice(0, length))
	}
	f.refResolver.Reference(value)

	// 分派读取逻辑
	if (collectFlag & CollectionNotSameType) == 0 {
		return s.readSameType(f, buf, value, elemTypeInfo, collectFlag)
	}
	return s.readDifferentTypes(f, buf, value)
}

func (s sliceSerializer) readSameType(f *Fury, buf *ByteBuffer, value reflect.Value, typeInfo TypeInfo, flag int8) error {
	trackRefs := (flag & CollectionTrackingRef) != 0
	serializer := typeInfo.Serializer
	var refID int32

	for i := 0; i < value.Len(); i++ {
		if trackRefs {
			refID, _ = f.refResolver.TryPreserveRefId(buf)
			if int8(refID) < NotNullValueFlag {
				value.Index(i).Set(f.refResolver.GetCurrentReadObject())
				continue
			}
		}

		// 创建正确类型的新值
		elem := reflect.New(typeInfo.Type).Elem()
		if err := serializer.Read(f, buf, elem.Type(), elem); err != nil {
			return err
		}
		value.Index(i).Set(elem)
		f.refResolver.SetReadObject(refID, elem)
	}
	return nil
}

func (s sliceSerializer) readDifferentTypes(f *Fury, buf *ByteBuffer, value reflect.Value) error {
	for i := 0; i < value.Len(); i++ {
		refID, _ := f.refResolver.TryPreserveRefId(buf)
		if int8(refID) < NotNullValueFlag {
			value.Index(i).Set(f.refResolver.GetCurrentReadObject())
			continue
		}
		typeID := buf.ReadVarInt32()
		typeInfo, _ := f.typeResolver.getTypeInfoById(int16(typeID))

		elem := reflect.New(typeInfo.Type).Elem()
		if err := typeInfo.Serializer.Read(f, buf, typeInfo.Type, elem); err != nil {
			return err
		}
		f.refResolver.SetReadObject(refID, elem)
		value.Index(i).Set(elem)
	}
	return nil
}

// 辅助函数
func isNull(v reflect.Value) bool {
	switch v.Kind() {
	case reflect.Ptr, reflect.Interface, reflect.Slice, reflect.Map, reflect.Func:
		return v.IsNil()
	default:
		return false
	}
}

// sliceConcreteValueSerializer serialize a slice whose elem is not an interface or pointer to interface
type sliceConcreteValueSerializer struct {
	type_          reflect.Type
	elemSerializer Serializer
	referencable   bool
}

func (s *sliceConcreteValueSerializer) TypeId() TypeId {
	return -LIST
}

func (s *sliceConcreteValueSerializer) Write(f *Fury, buf *ByteBuffer, value reflect.Value) error {
	length := value.Len()
	if err := f.writeLength(buf, length); err != nil {
		return err
	}

	var prevType reflect.Type
	for i := 0; i < length; i++ {
		elem := value.Index(i)
		elemType := elem.Type()

		var elemSerializer Serializer
		if i == 0 || elemType != prevType {
			elemSerializer = nil
		} else {
			elemSerializer = s.elemSerializer
		}

		if err := writeBySerializer(f, buf, elem, elemSerializer, s.referencable); err != nil {
			return err
		}

		prevType = elemType
	}
	return nil
}

func (s *sliceConcreteValueSerializer) Read(f *Fury, buf *ByteBuffer, type_ reflect.Type, value reflect.Value) error {
	length := f.readLength(buf)
	if value.Cap() < length {
		value.Set(reflect.MakeSlice(value.Type(), length, length))
	} else if value.Len() < length {
		value.Set(value.Slice(0, length))
	}
	f.refResolver.Reference(value)
	var prevType reflect.Type
	for i := 0; i < length; i++ {

		elem := value.Index(i)
		elemType := elem.Type()

		var elemSerializer Serializer
		if i == 0 || elemType != prevType {
			elemSerializer = nil
		} else {
			elemSerializer = s.elemSerializer
		}
		if err := readBySerializer(f, buf, value.Index(i), elemSerializer, s.referencable); err != nil {
			return err
		}
	}
	return nil
}

type byteSliceSerializer struct {
}

func (s byteSliceSerializer) TypeId() TypeId {
	return BINARY
}

func (s byteSliceSerializer) Write(f *Fury, buf *ByteBuffer, value reflect.Value) error {
	if err := f.WriteBufferObject(buf, &ByteSliceBufferObject{value.Interface().([]byte)}); err != nil {
		return err
	}
	return nil
}

func (s byteSliceSerializer) Read(f *Fury, buf *ByteBuffer, type_ reflect.Type, value reflect.Value) error {
	object, err := f.ReadBufferObject(buf)
	if err != nil {
		return err
	}
	value.Set(reflect.ValueOf(object.GetData()))
	return nil
}

type ByteSliceBufferObject struct {
	data []byte
}

func (o *ByteSliceBufferObject) TotalBytes() int {
	return len(o.data)
}

func (o *ByteSliceBufferObject) WriteTo(buf *ByteBuffer) {
	buf.WriteBinary(o.data)
}

func (o *ByteSliceBufferObject) ToBuffer() *ByteBuffer {
	return NewByteBuffer(o.data)
}

type boolSliceSerializer struct {
}

func (s boolSliceSerializer) TypeId() TypeId {
	return BOOL_ARRAY
}

func (s boolSliceSerializer) Write(f *Fury, buf *ByteBuffer, value reflect.Value) error {
	v := value.Interface().([]bool)
	size := len(v)
	if size >= MaxInt32 {
		return fmt.Errorf("too long slice: %d", len(v))
	}
	buf.WriteLength(size)
	for _, elem := range v {
		buf.WriteBool(elem)
	}
	return nil
}

func (s boolSliceSerializer) Read(f *Fury, buf *ByteBuffer, type_ reflect.Type, value reflect.Value) error {
	length := buf.ReadLength()
	r := make([]bool, length, length)
	for i := 0; i < length; i++ {
		r[i] = buf.ReadBool()
	}
	value.Set(reflect.ValueOf(r))
	return nil
}

type int16SliceSerializer struct {
}

func (s int16SliceSerializer) TypeId() TypeId {
	return INT16_ARRAY
}

func (s int16SliceSerializer) Write(f *Fury, buf *ByteBuffer, value reflect.Value) error {
	v := value.Interface().([]int16)
	size := len(v) * 2
	if size >= MaxInt32 {
		return fmt.Errorf("too long slice: %d", len(v))
	}
	buf.WriteLength(size)
	for _, elem := range v {
		buf.WriteInt16(elem)
	}
	return nil
}

func (s int16SliceSerializer) Read(f *Fury, buf *ByteBuffer, type_ reflect.Type, value reflect.Value) error {
	length := buf.ReadLength() / 2
	r := make([]int16, length, length)
	for i := 0; i < length; i++ {
		r[i] = buf.ReadInt16()
	}
	value.Set(reflect.ValueOf(r))
	return nil
}

type int32SliceSerializer struct {
}

func (s int32SliceSerializer) TypeId() TypeId {
	return INT32_ARRAY
}

func (s int32SliceSerializer) Write(f *Fury, buf *ByteBuffer, value reflect.Value) error {
	v := value.Interface().([]int32)
	size := len(v) * 4
	if size >= MaxInt32 {
		return fmt.Errorf("too long slice: %d", len(v))
	}
	buf.WriteLength(size)
	for _, elem := range v {
		buf.WriteInt32(elem)
	}
	return nil
}

func (s int32SliceSerializer) Read(f *Fury, buf *ByteBuffer, type_ reflect.Type, value reflect.Value) error {
	length := buf.ReadLength() / 4
	r := make([]int32, length, length)
	for i := 0; i < length; i++ {
		r[i] = buf.ReadInt32()
	}
	value.Set(reflect.ValueOf(r))
	return nil
}

type int64SliceSerializer struct {
}

func (s int64SliceSerializer) TypeId() TypeId {
	return INT64_ARRAY
}

func (s int64SliceSerializer) Write(f *Fury, buf *ByteBuffer, value reflect.Value) error {
	v := value.Interface().([]int64)
	size := len(v) * 8
	if size >= MaxInt32 {
		return fmt.Errorf("too long slice: %d", len(v))
	}
	buf.WriteLength(size)
	for _, elem := range v {
		buf.WriteInt64(elem)
	}
	return nil
}

func (s int64SliceSerializer) Read(f *Fury, buf *ByteBuffer, type_ reflect.Type, value reflect.Value) error {
	length := buf.ReadLength() / 8
	r := make([]int64, length, length)
	for i := 0; i < length; i++ {
		r[i] = buf.ReadInt64()
	}
	value.Set(reflect.ValueOf(r))
	return nil
}

type float32SliceSerializer struct {
}

func (s float32SliceSerializer) TypeId() TypeId {
	return FLOAT32_ARRAY
}

func (s float32SliceSerializer) Write(f *Fury, buf *ByteBuffer, value reflect.Value) error {
	v := value.Interface().([]float32)
	size := len(v) * 4
	if size >= MaxInt32 {
		return fmt.Errorf("too long slice: %d", len(v))
	}
	buf.WriteLength(size)
	for _, elem := range v {
		buf.WriteFloat32(elem)
	}
	return nil
}

func (s float32SliceSerializer) Read(f *Fury, buf *ByteBuffer, type_ reflect.Type, value reflect.Value) error {
	length := buf.ReadLength() / 4
	r := make([]float32, length, length)
	for i := 0; i < length; i++ {
		r[i] = buf.ReadFloat32()
	}
	value.Set(reflect.ValueOf(r))
	return nil
}

type float64SliceSerializer struct {
}

func (s float64SliceSerializer) TypeId() TypeId {
	return FLOAT64_ARRAY
}

func (s float64SliceSerializer) Write(f *Fury, buf *ByteBuffer, value reflect.Value) error {
	v := value.Interface().([]float64)
	size := len(v) * 8
	if size >= MaxInt32 {
		return fmt.Errorf("too long slice: %d", len(v))
	}
	buf.WriteLength(size)
	for _, elem := range v {
		buf.WriteFloat64(elem)
	}
	return nil
}

func (s float64SliceSerializer) Read(f *Fury, buf *ByteBuffer, type_ reflect.Type, value reflect.Value) error {
	length := buf.ReadLength() / 8
	r := make([]float64, length, length)
	for i := 0; i < length; i++ {
		r[i] = buf.ReadFloat64()
	}
	value.Set(reflect.ValueOf(r))
	return nil
}

type stringSliceSerializer struct {
	strSerializer stringSerializer
}

func (s stringSliceSerializer) TypeId() TypeId {
	return FURY_STRING_ARRAY
}

func (s stringSliceSerializer) Write(f *Fury, buf *ByteBuffer, value reflect.Value) error {
	v := value.Interface().([]string)
	err := f.writeLength(buf, len(v))
	if err != nil {
		return err
	}
	for _, str := range v {
		if refWritten, err := f.refResolver.WriteRefOrNull(buf, reflect.ValueOf(str)); err == nil {
			if !refWritten {
				if err := writeString(buf, str); err != nil {
					return err
				}
			}
		} else {
			return err
		}
	}
	return nil
}

func (s stringSliceSerializer) Read(f *Fury, buf *ByteBuffer, type_ reflect.Type, value reflect.Value) (err error) {
	length := f.readLength(buf)
	r := make([]string, length, length)
	f.refResolver.Reference(reflect.ValueOf(r))
	for i := 0; i < length; i++ {
		if refFlag := f.refResolver.ReadRefOrNull(buf); refFlag == RefValueFlag || refFlag == NotNullValueFlag {
			var nextReadRefId int32
			if refFlag == RefValueFlag {
				nextReadRefId, err = f.refResolver.PreserveRefId()
				if err != nil {
					return err
				}
			}
			elem := readString(buf)
			if f.referenceTracking && refFlag == RefValueFlag {
				// If value is not nil(reflect), then value is a pointer to some variable, we can update the `value`,
				// then record `value` in the reference resolver.
				f.refResolver.SetReadObject(nextReadRefId, reflect.ValueOf(elem))
			}
			r[i] = elem
		} else if refFlag == NullFlag {
			r[i] = ""
		} else {
			r[i] = f.refResolver.GetCurrentReadObject().Interface().(string)
		}
	}
	value.Set(reflect.ValueOf(r))
	return nil
}

// those types will be serialized by `sliceConcreteValueSerializer`, which correspond to List types in java/python

type Int8Slice []int8
type Int16Slice []int16
type Int32Slice []int32
type Int64Slice []int64
type Float32Slice []float64
type Float64Slice []float64
