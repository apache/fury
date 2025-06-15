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

package fory

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

func (s sliceSerializer) NeedWriteRef() bool {
	return true
}

// Write serializes a slice value into the buffer
func (s sliceSerializer) Write(f *Fory, buf *ByteBuffer, value reflect.Value) error {
	// Get slice length and handle empty slice case
	length := value.Len()
	if length == 0 {
		buf.WriteVarUint32(0) // Write 0 for empty slice
		return nil
	}

	// Write collection header and get type information
	collectFlag, elemTypeInfo := s.writeHeader(f, buf, value)

	// Choose serialization path based on type consistency
	if (collectFlag & CollectionNotSameType) == 0 {
		return s.writeSameType(f, buf, value, elemTypeInfo, collectFlag) // Optimized path for same-type elements
	}
	return s.writeDifferentTypes(f, buf, value) // Fallback path for mixed-type elements
}

// writeHeader prepares and writes collection metadata including:
// - Collection size
// - Type consistency flags
// - Element type information (if homogeneous)
func (s sliceSerializer) writeHeader(f *Fory, buf *ByteBuffer, value reflect.Value) (byte, TypeInfo) {
	collectFlag := CollectionDefaultFlag
	var elemTypeInfo TypeInfo
	hasNull := false
	hasDifferentType := false

	// Get type information for the first element
	elemTypeInfo, _ = f.typeResolver.getTypeInfo(value, true)
	collectFlag |= CollectionNotDeclElementType

	// Iterate through elements to check for nulls and type consistency
	for i := 0; i < value.Len(); i++ {
		elem := value.Index(i).Elem()
		if isNull(elem) {
			hasNull = true
			continue
		}

		// Compare each element's type with the first element's type
		currentTypeInfo, _ := f.typeResolver.getTypeInfo(elem, true)
		if currentTypeInfo.TypeID != elemTypeInfo.TypeID {
			hasDifferentType = true
		}
	}

	// Set collection flags based on findings
	if hasNull {
		collectFlag |= CollectionHasNull // Mark if collection contains null values
	}
	if hasDifferentType {
		collectFlag |= CollectionNotSameType // Mark if elements have different types
	}

	// Enable reference tracking if configured
	if f.referenceTracking {
		collectFlag |= CollectionTrackingRef
	}

	// Write metadata to buffer
	buf.WriteVarUint32(uint32(value.Len())) // Collection size
	buf.WriteInt8(int8(collectFlag))        // Collection flags

	// Write element type ID if all elements have same type
	if !hasDifferentType {
		buf.WriteVarInt32(elemTypeInfo.TypeID)
	}

	return byte(collectFlag), elemTypeInfo
}

// writeSameType efficiently serializes a slice where all elements share the same type
func (s sliceSerializer) writeSameType(f *Fory, buf *ByteBuffer, value reflect.Value, typeInfo TypeInfo, flag byte) error {
	serializer := typeInfo.Serializer
	trackRefs := (flag & CollectionTrackingRef) != 0 // Check if reference tracking is enabled

	for i := 0; i < value.Len(); i++ {
		elem := value.Index(i).Elem()
		if isNull(elem) {
			buf.WriteInt8(NullFlag) // Write null marker
			continue
		}

		if trackRefs {
			// Handle reference tracking if enabled
			refWritten, err := f.refResolver.WriteRefOrNull(buf, elem)
			if err != nil {
				return err
			}
			if !refWritten {
				// Write actual value if not a reference
				if err := serializer.Write(f, buf, elem); err != nil {
					return err
				}
			}
		} else {
			// Directly write value without reference tracking
			if err := serializer.Write(f, buf, elem); err != nil {
				return err
			}
		}
	}
	return nil
}

// writeDifferentTypes handles serialization of slices with mixed element types
func (s sliceSerializer) writeDifferentTypes(f *Fory, buf *ByteBuffer, value reflect.Value) error {
	for i := 0; i < value.Len(); i++ {
		elem := value.Index(i).Elem()
		if isNull(elem) {
			buf.WriteInt8(NullFlag) // Write null marker
			continue
		}

		// Handle reference tracking
		refWritten, err := f.refResolver.WriteRefOrNull(buf, elem)
		if err != nil {
			return err
		}
		if refWritten {
			continue // Skip if element was written as reference
		}

		// Get and write type info for each element (since types vary)
		typeInfo, _ := f.typeResolver.getTypeInfo(elem, true)
		buf.WriteVarInt32(typeInfo.TypeID)

		// Write actual value
		if err := typeInfo.Serializer.Write(f, buf, elem); err != nil {
			return err
		}
	}
	return nil
}

// Read deserializes a slice from the buffer into the provided reflect.Value
func (s sliceSerializer) Read(f *Fory, buf *ByteBuffer, type_ reflect.Type, value reflect.Value) error {
	// Read slice length from buffer
	length := int(buf.ReadVarUint32())
	if length == 0 {
		// Initialize empty slice if length is 0
		value.Set(reflect.MakeSlice(type_, 0, 0))
		return nil
	}

	// Read collection flags that indicate special characteristics
	collectFlag := buf.ReadInt8()
	var elemTypeInfo TypeInfo

	// Read element type information if all elements are same type
	if (collectFlag & CollectionNotSameType) == 0 {
		typeID := buf.ReadVarInt32()
		elemTypeInfo, _ = f.typeResolver.getTypeInfoById(int16(typeID))
	}

	// Initialize slice with proper capacity
	if value.Cap() < length {
		value.Set(reflect.MakeSlice(type_, length, length))
	} else {
		value.Set(value.Slice(0, length))
	}
	// Register reference for tracking
	f.refResolver.Reference(value)

	// Choose appropriate deserialization path based on type consistency
	if (collectFlag & CollectionNotSameType) == 0 {
		return s.readSameType(f, buf, value, elemTypeInfo, collectFlag)
	}
	return s.readDifferentTypes(f, buf, value)
}

// readSameType handles deserialization of slices where all elements share the same type
func (s sliceSerializer) readSameType(f *Fory, buf *ByteBuffer, value reflect.Value, typeInfo TypeInfo, flag int8) error {
	// Determine if reference tracking is enabled
	trackRefs := (flag & CollectionTrackingRef) != 0
	serializer := typeInfo.Serializer
	var refID int32

	for i := 0; i < value.Len(); i++ {
		if trackRefs {
			// Handle reference tracking if enabled
			refID, _ = f.refResolver.TryPreserveRefId(buf)
			if int8(refID) < NotNullValueFlag {
				// Use existing reference if available
				value.Index(i).Set(f.refResolver.GetCurrentReadObject())
				continue
			}
		}

		// Create new element of the correct type and deserialize from buffer
		elem := reflect.New(typeInfo.Type).Elem()
		if err := serializer.Read(f, buf, elem.Type(), elem); err != nil {
			return err
		}
		// Set element in slice and register reference
		value.Index(i).Set(elem)
		f.refResolver.SetReadObject(refID, elem)
	}
	return nil
}

// readDifferentTypes handles deserialization of slices with mixed element types
func (s sliceSerializer) readDifferentTypes(f *Fory, buf *ByteBuffer, value reflect.Value) error {
	for i := 0; i < value.Len(); i++ {
		// Handle reference tracking for each element
		refID, _ := f.refResolver.TryPreserveRefId(buf)
		if int8(refID) < NotNullValueFlag {
			// Use existing reference if available
			value.Index(i).Set(f.refResolver.GetCurrentReadObject())
			continue
		}

		// Read type ID for each element (since types vary)
		typeID := buf.ReadVarInt32()
		typeInfo, _ := f.typeResolver.getTypeInfoById(int16(typeID))

		// Create new element and deserialize from buffer
		elem := reflect.New(typeInfo.Type).Elem()
		if err := typeInfo.Serializer.Read(f, buf, typeInfo.Type, elem); err != nil {
			return err
		}
		// Set element in slice and register reference
		f.refResolver.SetReadObject(refID, elem)
		value.Index(i).Set(elem)
	}
	return nil
}

// Helper function to check if a value is null/nil
func isNull(v reflect.Value) bool {
	switch v.Kind() {
	case reflect.Ptr, reflect.Interface, reflect.Slice, reflect.Map, reflect.Func:
		return v.IsNil() // Check if reference types are nil
	default:
		return false // Value types are never null
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

func (s sliceConcreteValueSerializer) NeedWriteRef() bool {
	return true
}

func (s *sliceConcreteValueSerializer) Write(f *Fory, buf *ByteBuffer, value reflect.Value) error {
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

func (s *sliceConcreteValueSerializer) Read(f *Fory, buf *ByteBuffer, type_ reflect.Type, value reflect.Value) error {
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

func (s byteSliceSerializer) NeedWriteRef() bool {
	return true
}

func (s byteSliceSerializer) Write(f *Fory, buf *ByteBuffer, value reflect.Value) error {
	if err := f.WriteBufferObject(buf, &ByteSliceBufferObject{value.Interface().([]byte)}); err != nil {
		return err
	}
	return nil
}

func (s byteSliceSerializer) Read(f *Fory, buf *ByteBuffer, type_ reflect.Type, value reflect.Value) error {
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

func (s boolSliceSerializer) NeedWriteRef() bool {
	return true
}

func (s boolSliceSerializer) Write(f *Fory, buf *ByteBuffer, value reflect.Value) error {
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

func (s boolSliceSerializer) Read(f *Fory, buf *ByteBuffer, type_ reflect.Type, value reflect.Value) error {
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

func (s int16SliceSerializer) NeedWriteRef() bool {
	return true
}

func (s int16SliceSerializer) Write(f *Fory, buf *ByteBuffer, value reflect.Value) error {
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

func (s int16SliceSerializer) Read(f *Fory, buf *ByteBuffer, type_ reflect.Type, value reflect.Value) error {
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

func (s int32SliceSerializer) NeedWriteRef() bool {
	return true
}

func (s int32SliceSerializer) Write(f *Fory, buf *ByteBuffer, value reflect.Value) error {
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

func (s int32SliceSerializer) Read(f *Fory, buf *ByteBuffer, type_ reflect.Type, value reflect.Value) error {
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

func (s int64SliceSerializer) NeedWriteRef() bool {
	return true
}

func (s int64SliceSerializer) Write(f *Fory, buf *ByteBuffer, value reflect.Value) error {
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

func (s int64SliceSerializer) Read(f *Fory, buf *ByteBuffer, type_ reflect.Type, value reflect.Value) error {
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

func (s float32SliceSerializer) NeedWriteRef() bool {
	return true
}

func (s float32SliceSerializer) Write(f *Fory, buf *ByteBuffer, value reflect.Value) error {
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

func (s float32SliceSerializer) Read(f *Fory, buf *ByteBuffer, type_ reflect.Type, value reflect.Value) error {
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

func (s float64SliceSerializer) NeedWriteRef() bool {
	return true
}

func (s float64SliceSerializer) Write(f *Fory, buf *ByteBuffer, value reflect.Value) error {
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

func (s float64SliceSerializer) Read(f *Fory, buf *ByteBuffer, type_ reflect.Type, value reflect.Value) error {
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
	return FORY_STRING_ARRAY
}

func (s stringSliceSerializer) NeedWriteRef() bool {
	return true
}

func (s stringSliceSerializer) Write(f *Fory, buf *ByteBuffer, value reflect.Value) error {
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

func (s stringSliceSerializer) Read(f *Fory, buf *ByteBuffer, type_ reflect.Type, value reflect.Value) (err error) {
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
