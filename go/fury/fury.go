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
	"encoding/binary"
	"fmt"
	"reflect"
	"sync"
)

func NewFury(referenceTracking bool) *Fury {
	fury := &Fury{
		refResolver:       newRefResolver(referenceTracking),
		referenceTracking: referenceTracking,
		language:          XLANG,
		buffer:            NewByteBuffer(nil),
	}
	fury.typeResolver = newTypeResolver(fury)
	return fury
}

var furyPool = sync.Pool{
	New: func() interface{} {
		return NewFury(true)
	},
}

func GetFury() *Fury {
	return furyPool.Get().(*Fury)
}

func PutFury(fury *Fury) {
	furyPool.Put(fury)
}

// Marshal returns the MessagePack encoding of v.
func Marshal(v interface{}) ([]byte, error) {
	fury := GetFury()
	err := fury.Serialize(nil, v, nil)
	data := fury.buffer.GetByteSlice(0, fury.buffer.writerIndex)
	PutFury(fury)
	if err != nil {
		return nil, err
	}
	return data, err
}

// Unmarshal decodes the fury-encoded data and stores the result
// in the value pointed to by v.
func Unmarshal(data []byte, v interface{}) error {
	fury := GetFury()
	err := fury.Deserialize(NewByteBuffer(data), v, nil)
	PutFury(fury)
	return err
}

// BufferCallback to check whether write buffer in band. If the callback returns false, the given buffer is
// out-of-band; otherwise the buffer is serialized in-band, i.e. inside the serialized stream.
type BufferCallback = func(o BufferObject) bool

type Language = uint8

const (
	XLANG Language = iota
	JAVA
	PYTHON
	CPP
	GO
	JAVASCRIPT
	RUST
	DART
)

const (
	isNilFlag byte = 1 << iota
	isLittleEndianFlag
	isCrossLanguageFlag
	isOutOfBandFlag
)

const (
	NilFlag          = 0
	LittleEndianFlag = 2
	XLangFlag        = 4
	CallBackFlag     = 8
)

const MAGIC_NUMBER int16 = 0x62D4

type Fury struct {
	typeResolver      *typeResolver
	refResolver       *RefResolver
	referenceTracking bool
	language          Language
	bufferCallback    BufferCallback
	peerLanguage      Language
	buffer            *ByteBuffer
	buffers           []*ByteBuffer
}

func (f *Fury) RegisterTagType(tag string, v interface{}) error {
	return f.typeResolver.RegisterTypeTag(reflect.TypeOf(v), tag)
}

func (f *Fury) Marshal(v interface{}) ([]byte, error) {
	err := f.Serialize(nil, v, nil)
	if err != nil {
		return nil, err
	}
	return f.buffer.GetByteSlice(0, f.buffer.writerIndex), nil
}

func (f *Fury) Serialize(buf *ByteBuffer, v interface{}, callback BufferCallback) error {
	defer f.resetWrite()
	f.bufferCallback = callback
	buffer := buf
	if buffer == nil {
		buffer = f.buffer
		buffer.writerIndex = 0
	}
	if f.language == XLANG {
		buffer.WriteInt16(MAGIC_NUMBER)
	} else {
		return fmt.Errorf("%d language is not supported", f.language)
	}
	var bitmap byte = 0
	if isNil(reflect.ValueOf(v)) {
		bitmap |= NilFlag
	}
	if nativeEndian == binary.LittleEndian {
		bitmap |= LittleEndianFlag
	}
	// set reader as x_lang.
	if f.language == XLANG {
		bitmap |= XLangFlag
	} else {
		return fmt.Errorf("%d language is not supported", f.language)
	}
	if callback != nil {
		bitmap |= CallBackFlag
	}
	if err := buffer.WriteByte(bitmap); err != nil {
		return err
	}
	if f.language != XLANG {
		return fmt.Errorf("%d language is not supported", f.language)
	} else {
		if err := buffer.WriteByte(GO); err != nil {
			return err
		}
		if err := f.Write(buffer, v); err != nil {
			return err
		}
	}
	return nil
}

func (f *Fury) Write(buffer *ByteBuffer, v interface{}) (err error) {
	// fast path for common type
	switch v := v.(type) {
	case nil:
		buffer.WriteInt8(NullFlag)
	case bool:
		f.WriteBool(buffer, v)
	case float64:
		f.WriteFloat64(buffer, v)
	case float32:
		f.WriteFloat32(buffer, v)
	case byte: // uint8
		f.WriteByte_(buffer, v)
	default:
		err = f.WriteReferencable(buffer, reflect.ValueOf(v))
	}
	return
}

func (f *Fury) WriteByte_(buffer *ByteBuffer, v interface{}) {
	buffer.WriteInt8(NotNullValueFlag)
	buffer.WriteInt8(UINT8)
	buffer.WriteByte_(v.(byte))
}

func (f *Fury) WriteInt16(buffer *ByteBuffer, v interface{}) {
	buffer.WriteInt8(NotNullValueFlag)
	buffer.WriteInt8(INT32)
	buffer.WriteInt32(v.(int32))
}

func (f *Fury) WriteBool(buffer *ByteBuffer, v interface{}) {
	buffer.WriteInt8(NotNullValueFlag)
	buffer.WriteInt8(BOOL)
	buffer.WriteBool(v.(bool))
}

func (f *Fury) WriteInt32(buffer *ByteBuffer, v interface{}) {
	buffer.WriteInt8(NotNullValueFlag)
	buffer.WriteInt8(INT32)
	buffer.WriteInt32(v.(int32))
}

func (f *Fury) WriteInt64(buffer *ByteBuffer, v interface{}) {
	buffer.WriteInt8(NotNullValueFlag)
	buffer.WriteInt8(INT64)
	buffer.WriteInt64(v.(int64))
}

func (f *Fury) WriteFloat32(buffer *ByteBuffer, v interface{}) {
	buffer.WriteInt8(NotNullValueFlag)
	buffer.WriteInt8(FLOAT)
	buffer.WriteFloat32(v.(float32))
}

func (f *Fury) WriteFloat64(buffer *ByteBuffer, v interface{}) {
	buffer.WriteInt8(NotNullValueFlag)
	buffer.WriteInt8(DOUBLE)
	buffer.WriteFloat64(v.(float64))
}

func (f *Fury) writeLength(buffer *ByteBuffer, value int) error {
	if value > MaxInt32 || value < MinInt32 {
		return fmt.Errorf("value %d exceed the int32 range", value)
	}
	buffer.WriteVarInt32(int32(value))
	return nil
}

func (f *Fury) readLength(buffer *ByteBuffer) int {
	return int(buffer.ReadVarInt32())
}

func (f *Fury) WriteReferencable(buffer *ByteBuffer, value reflect.Value) error {
	return f.writeReferencableBySerializer(buffer, value, nil)
}

func (f *Fury) writeReferencableBySerializer(buffer *ByteBuffer, value reflect.Value, serializer Serializer) error {
	if refWritten, err := f.refResolver.WriteRefOrNull(buffer, value); err == nil && !refWritten {
		// check ptr
		if value.Kind() == reflect.Ptr {
			switch value.Elem().Kind() {
			case reflect.Ptr, reflect.Map, reflect.Slice, reflect.Interface:
				return fmt.Errorf("pointer to reference type %s is not supported", value.Type())
			}
		}
		return f.writeValue(buffer, value, serializer)
	} else {
		return err
	}
}

func (f *Fury) writeNonReferencableBySerializer(
	buffer *ByteBuffer, value reflect.Value, serializer Serializer) error {
	buffer.WriteInt8(NotNullValueFlag)
	return f.writeValue(buffer, value, serializer)
}

func (f *Fury) writeValue(buffer *ByteBuffer, value reflect.Value, serializer Serializer) (err error) {
	// Handle interface values by getting their concrete element
	if value.Kind() == reflect.Interface {
		value = value.Elem()
	}

	// Get type information for the value
	typeInfo, err := f.typeResolver.getTypeInfo(value, true)
	type_ := typeInfo.Type

	// If no serializer provided, get one for the type
	if serializer == nil {
		serializer, err = f.typeResolver.getSerializerByType(type_)
		if err != nil {
			return err
		}
	}

	// Write type information to buffer
	err = f.typeResolver.writeTypeInfo(buffer, typeInfo)
	if err != nil {
		return err
	}

	// Serialize the actual value using the serializer
	return serializer.Write(f, buffer, value)
}

func (f *Fury) WriteBufferObject(buffer *ByteBuffer, bufferObject BufferObject) error {
	if f.bufferCallback == nil || f.bufferCallback(bufferObject) {
		buffer.WriteBool(true)
		size := bufferObject.TotalBytes()
		// writer length
		buffer.WriteLength(size)
		writerIndex := buffer.writerIndex
		buffer.grow(size)
		bufferObject.WriteTo(buffer.Slice(writerIndex, size))
		buffer.writerIndex += size
		if size > MaxInt32 {
			return fmt.Errorf("length %d exceed max int32", size)
		}
	} else {
		buffer.WriteBool(false)
	}
	return nil
}

func (f *Fury) Unmarshal(data []byte, v interface{}) error {
	return f.Deserialize(&ByteBuffer{data: data}, v, nil)
}

func (f *Fury) Deserialize(buf *ByteBuffer, v interface{}, buffers []*ByteBuffer) error {
	defer f.resetRead()
	if f.language == XLANG {
		magicNumber := buf.ReadInt16()
		if magicNumber != MAGIC_NUMBER {
			return fmt.Errorf(
				"the fury xlang serialization must start with magic number 0x%x. "+
					"Please check whether the serialization is based on the xlang protocol and the data didn't corrupt",
				MAGIC_NUMBER)
		}
	} else {
		return fmt.Errorf("%d language is not supported", f.language)
	}
	var bitmap = buf.ReadByte_()
	if bitmap&NilFlag != NilFlag {
		return nil
	}
	isLittleEndian := bitmap&LittleEndianFlag == LittleEndianFlag
	if !isLittleEndian {
		return fmt.Errorf("big endian is not supported for now, please ensure peer machine is little endian")
	}
	isXLangFlag := bitmap&XLangFlag == XLangFlag
	if isXLangFlag {
		f.peerLanguage = buf.ReadByte_()
	} else {
		f.peerLanguage = GO
	}
	isCallBackFlag := bitmap&CallBackFlag == CallBackFlag
	if isCallBackFlag {
		if buffers == nil {
			return fmt.Errorf("uffers shouldn't be null when the serialized stream is " +
				"produced with buffer_callback not null")
		}
		f.buffers = buffers
	} else {
		if buffers != nil {
			return fmt.Errorf("buffers should be null when the serialized stream is " +
				"produced with buffer_callback null")
		}
	}
	if isXLangFlag {
		return f.ReadReferencable(buf, reflect.ValueOf(v).Elem())
	} else {
		return fmt.Errorf("native serialization for golang is not supported currently")
	}
}

func (f *Fury) ReadReferencable(buffer *ByteBuffer, value reflect.Value) error {
	return f.readReferencableBySerializer(buffer, value, nil)
}

func (f *Fury) readReferencableBySerializer(buf *ByteBuffer, value reflect.Value, serializer Serializer) (err error) {
	refId, err := f.refResolver.TryPreserveRefId(buf)
	if err != nil {
		return err
	}
	if refId >= int32(NotNullValueFlag) {
		err = f.readData(buf, value, serializer)
		if err != nil {
			return err
		}
		// If value is not nil(reflect), then value is a pointer to some variable, we can update the `value`,
		// then record `value` in the reference resolver.
		if value.Kind() == reflect.Interface {
			// If same value read again, and type isn't interface, call `Set` will fail.
			// so we should save interface dynamic value.
			value = value.Elem()
		}
		f.refResolver.SetReadObject(refId, value)
		return nil
	} else {
		if refId == int32(NullFlag) {
			return nil
		}
		value.Set(f.refResolver.GetCurrentReadObject())
		return nil
	}
}

func (f *Fury) readData(buffer *ByteBuffer, value reflect.Value, serializer Serializer) (err error) {
	var typeInfo TypeInfo
	var type_ reflect.Type

	// Read type information from the buffer
	typeInfo, err = f.typeResolver.readTypeInfo(buffer)
	if serializer == nil {
		serializer = typeInfo.Serializer // Use serializer from type info if none provided
	}

	// Determine concrete type (use value's type if type info is nil)
	if typeInfo.Type == nil {
		type_ = value.Type()
	} else {
		type_ = typeInfo.Type
	}

	// Handle interface types specially
	if value.Kind() == reflect.Interface {
		// Create a new concrete value since interface elements aren't addressable
		newValue := reflect.New(type_).Elem()
		err := serializer.Read(f, buffer, type_, newValue)
		if err != nil {
			return err
		}
		// Set the populated concrete value into the interface
		value.Set(newValue)
		return nil
	} else {
		// For non-interface types, read directly into the value
		// (nil checks are handled by individual serializers)
		return serializer.Read(f, buffer, typeInfo.Type, value)
	}
}

func (f *Fury) ReadBufferObject(buffer *ByteBuffer) (*ByteBuffer, error) {
	isInBand := buffer.ReadBool()
	// TODO(chaokunyang) We need a way to wrap out-of-band buffer into byte slice without copy.
	// See more at `https://github.com/golang/go/wiki/cgo#turning-c-arrays-into-go-slices`
	if isInBand {
		size := buffer.ReadLength()
		buf := buffer.Slice(buffer.readerIndex, size)
		buffer.readerIndex += size
		return buf, nil
	} else {
		if f.buffers == nil {
			return nil, fmt.Errorf("buffers shouldn't be nil when met a out-of-band buffer")
		}
		buf := f.buffers[0]
		f.buffers = f.buffers[1:]
		return buf, nil
	}
}

func (f *Fury) Reset() {
	f.resetWrite()
	f.resetRead()
}

func (f *Fury) resetWrite() {
	f.typeResolver.resetWrite()
	f.refResolver.resetWrite()
}

func (f *Fury) resetRead() {
	f.typeResolver.resetRead()
	f.refResolver.resetRead()
}

// methods for configure fury.

func (f *Fury) SetLanguage(language Language) {
	f.language = language
}

func (f *Fury) SetReferenceTracking(referenceTracking bool) {
	f.referenceTracking = referenceTracking
}
