// Copyright 2023 The Fury Authors
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package fury

import (
	"fmt"
	"reflect"
	"unsafe"
)

const (
	NullFlag int8 = -3
	// RefFlag indicates that object is a not-null value.
	// We don't use another byte to indicate REF, so that we can save one byte.
	RefFlag int8 = -2
	// NotNullValueFlag indicates that the object is a non-null value.
	NotNullValueFlag int8 = -1
	// RefValueFlag indicates that the object is a referencable and first read.
	RefValueFlag int8 = 0
)

// ReferenceResolver class is used to track objects that have already been read or written.
type ReferenceResolver struct {
	referenceTracking bool
	writtenObjects    map[referenceKey]int32
	readObjects       []reflect.Value
	readReferenceIds  []int32
	readObject        reflect.Value // last read object which is not a reference
}

type referenceKey struct {
	pointer unsafe.Pointer
	length  int // for slice and *array only
}

func newReferenceResolver(referenceTracking bool) *ReferenceResolver {
	referenceResolver := &ReferenceResolver{
		referenceTracking: referenceTracking,
		writtenObjects:    map[referenceKey]int32{},
	}
	return referenceResolver
}

// WriteReferenceOrNull write reference and tag for the value if the value has been written previously,
// write null/not-null tag otherwise. Returns true if no bytes need to be written for the object.
// See https://go101.org/article/value-part.html for internal structure definitions of common types.
// Note that for slice and substring, if the start addr or length are different, we take two objects as
// different references.
func (r *ReferenceResolver) WriteReferenceOrNull(buffer *ByteBuffer, value reflect.Value) (refWritten bool, err error) {
	if !r.referenceTracking {
		if isNil(value) {
			buffer.WriteInt8(NullFlag)
			return true, nil
		} else {
			buffer.WriteInt8(NotNullValueFlag)
			return false, nil
		}
	}
	length := 0
	isNil := false
	kind := value.Kind()
	// reference types such as channel/function are not handled here and will be handled by typeResolver.
	switch kind {
	case reflect.Ptr:
		elemValue := value.Elem()
		if elemValue.Kind() == reflect.Array {
			length = elemValue.Len()
		}
		isNil = value.IsNil()
	case reflect.Map:
		isNil = value.IsNil()
	case reflect.Slice:
		isNil = value.IsNil()
		length = value.Len()
	case reflect.Interface:
		value = value.Elem()
		return r.WriteReferenceOrNull(buffer, value)
	case reflect.String:
		isNil = false
		str := unsafeGetBytes(value.Interface().(string))
		value = reflect.ValueOf(str)
		length = len(str)
	case reflect.Invalid:
		isNil = true
	default:
		// The object is being written for the first time.
		buffer.WriteInt8(NotNullValueFlag)
		return false, nil
	}
	if isNil {
		buffer.WriteInt8(NullFlag)
		return true, nil
	} else {
		refKey := referenceKey{pointer: unsafe.Pointer(value.Pointer()), length: length}
		if writtenId, ok := r.writtenObjects[refKey]; ok {
			// The obj has been written previously.
			buffer.WriteInt8(RefFlag)
			buffer.WriteVarInt32(writtenId)
			return true, nil
		} else {
			// The id should be consistent with `nextReadRefId`
			newWriteRefId := len(r.writtenObjects)
			if newWriteRefId >= MaxInt32 {
				return false, fmt.Errorf("too many objects execced %d to serialize", MaxInt32)
			}
			r.writtenObjects[refKey] = int32(newWriteRefId)
			buffer.WriteInt8(RefValueFlag)
			return false, nil
		}
	}
}

// ReadReferenceOrNull returns RefFlag if a Reference to a previously read object
// was read. Returns NullFlag if the object is null. Returns RefValueFlag if the object is not
// null and Reference tracking is not enabled or the object is first read.
func (r *ReferenceResolver) ReadReferenceOrNull(buffer *ByteBuffer) int8 {
	refTag := buffer.ReadInt8()
	if !r.referenceTracking {
		return refTag
	}
	if refTag == RefFlag {
		// read reference id and get object from reference resolver
		refId := buffer.ReadVarInt32()
		r.readObject = r.GetReadObject(refId)
		return RefFlag
	} else {
		r.readObject = reflect.Value{}
	}
	return refTag
}

// PreserveReferenceId preserve a reference id, which is used by Reference / SetReadObject to
// set up reference for object that is first deserialized.
// Returns a reference id or -1 if reference is not enabled.
func (r *ReferenceResolver) PreserveReferenceId() (int32, error) {
	if !r.referenceTracking {
		return -1, nil
	}
	nextReadRefId_ := len(r.readObjects)
	if nextReadRefId_ > MaxInt32 {
		return 0, fmt.Errorf("referencable objects exceeds max int32")
	}
	nextReadRefId := int32(nextReadRefId_)
	r.readObjects = append(r.readObjects, reflect.Value{})
	r.readReferenceIds = append(r.readReferenceIds, nextReadRefId)
	return nextReadRefId, nil
}

func (r *ReferenceResolver) TryPreserveReferenceId(buffer *ByteBuffer) (int32, error) {
	headFlag := buffer.ReadInt8()
	if headFlag == RefFlag {
		// read reference id and get object from reference resolver
		refId := buffer.ReadVarInt32()
		r.readObject = r.GetReadObject(refId)
	} else {
		r.readObject = reflect.Value{}
		if headFlag == RefValueFlag {
			return r.PreserveReferenceId()
		}
	}
	// `headFlag` except `REF_FLAG` can be used as stub reference id because we use
	// `refId >= NOT_NULL_VALUE_FLAG` to read data.
	return int32(headFlag), nil
}

// Reference tracking references relationship. Call this method immediately after composited object such as
// object array/map/collection/bean is created so that circular reference can be deserialized correctly.
func (r *ReferenceResolver) Reference(value reflect.Value) {
	if !r.referenceTracking {
		return
	}
	length := len(r.readReferenceIds)
	refId := r.readReferenceIds[length-1]
	r.readReferenceIds = r.readReferenceIds[:length-1]
	r.SetReadObject(refId, value)
}

// GetReadObject returns the object for the specified id.
func (r *ReferenceResolver) GetReadObject(refId int32) reflect.Value {
	if !r.referenceTracking {
		return reflect.Value{}
	}
	return r.readObjects[refId]
}

func (r *ReferenceResolver) GetCurrentReadObject() reflect.Value {
	return r.readObject
}

// SetReadObject sets the id for an object that has been read.
// id: The id from {@link #NextReadRefId}.
// object: the object that has been read
func (r *ReferenceResolver) SetReadObject(refId int32, value reflect.Value) {
	if !r.referenceTracking {
		return
	}
	if refId >= 0 {
		r.readObjects[refId] = value
	}
}

func (r *ReferenceResolver) reset() {
	r.resetRead()
	r.resetWrite()
}

func (r *ReferenceResolver) resetRead() {
	if !r.referenceTracking {
		return
	}
	r.readObjects = nil
	r.readReferenceIds = nil
	r.readObject = reflect.Value{}
}

func (r *ReferenceResolver) resetWrite() {
	if len(r.writtenObjects) > 0 {
		r.writtenObjects = map[referenceKey]int32{}
	}
}

func nullable(type_ reflect.Type) bool {
	// Since we can't get value type from interface type, so we return true for interface type
	switch type_.Kind() {
	case reflect.Chan, reflect.Func, reflect.Map, reflect.Ptr, reflect.Slice, reflect.Interface, reflect.String:
		return true
	}
	return false
}

func isNil(value reflect.Value) bool {
	switch value.Kind() {
	case reflect.Chan, reflect.Func, reflect.Map, reflect.Ptr, reflect.Slice:
		return value.IsNil()
	case reflect.Interface:
		if value.IsValid() {
			return value.IsNil() || isNil(value.Elem())
		} else {
			return true
		}
	case reflect.Invalid:
		return true
	}
	return false
}
