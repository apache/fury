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
	"errors"
	"fmt"
	"reflect"
)

const (
	TRACKING_KEY_REF   = 1 << 0 // 0b00000001
	KEY_HAS_NULL       = 1 << 1 // 0b00000010
	KEY_DECL_TYPE      = 1 << 2 // 0b00000100
	TRACKING_VALUE_REF = 1 << 3 // 0b00001000
	VALUE_HAS_NULL     = 1 << 4 // 0b00010000
	VALUE_DECL_TYPE    = 1 << 5 // 0b00100000
	MAX_CHUNK_SIZE     = 255
)

const (
	KV_NULL                               = KEY_HAS_NULL | VALUE_HAS_NULL                       // 0b00010010
	NULL_KEY_VALUE_DECL_TYPE              = KEY_HAS_NULL | VALUE_DECL_TYPE                      // 0b00100010
	NULL_KEY_VALUE_DECL_TYPE_TRACKING_REF = KEY_HAS_NULL | VALUE_DECL_TYPE | TRACKING_VALUE_REF // 0b00101010
	NULL_VALUE_KEY_DECL_TYPE              = VALUE_HAS_NULL | KEY_DECL_TYPE                      // 0b00010100
	NULL_VALUE_KEY_DECL_TYPE_TRACKING_REF = VALUE_HAS_NULL | KEY_DECL_TYPE | TRACKING_KEY_REF   // 0b00010101
)

type mapSerializer struct {
}

func (s mapSerializer) TypeId() TypeId {
	return MAP
}

func (s mapSerializer) Write(f *Fory, buf *ByteBuffer, value reflect.Value) error {
	// Get map length and write it to buffer
	length := value.Len()
	buf.WriteVarUint32(uint32(length))
	if length == 0 {
		return nil
	}

	// Get resolvers from Fory instance
	typeResolver := f.typeResolver
	refResolver := f.refResolver
	var keySerializer Serializer
	var valueSerializer Serializer

	// Initialize map iterator and get first key-value pair
	iter := value.MapRange()
	if !iter.Next() {
		return nil
	}
	key, val := iter.Key(), iter.Value()
	hasNext := true

	for hasNext {
		// Process null key/value pairs
		for {
			keyValid := isValid(key)
			valValid := isValid(val)

			if keyValid && valValid {
				break
			}

			var header byte
			switch {
			case !keyValid && !valValid:
				header = KV_NULL
			case !keyValid:
				header = KEY_HAS_NULL | TRACKING_VALUE_REF
				if err := f.Write(buf, val); err != nil {
					return err
				}
			case !valValid:
				header = VALUE_HAS_NULL | TRACKING_KEY_REF
				if err := f.Write(buf, key); err != nil {
					return err
				}
			}
			buf.WriteInt8(int8(header))

			if iter.Next() {
				key, val = iter.Key(), iter.Value()
			} else {
				hasNext = false
				break
			}
		}

		if !hasNext {
			break
		}

		// Write chunk header placeholder (will be updated later)
		chunkHeaderOffset := buf.WriterIndex()
		buf.WriteInt16(-1)

		// Process type information
		chunkHeader := byte(0)
		if keySerializer == nil {
			// Get key type info and write to buffer
			keyTypeInfo, _ := getActualTypeInfo(key, typeResolver)
			if err := typeResolver.writeTypeInfo(buf, keyTypeInfo); err != nil {
				return err
			}
			keySerializer = keyTypeInfo.Serializer
		} else {
			chunkHeader |= KEY_DECL_TYPE // Key type already declared
		}

		if valueSerializer == nil {
			// Get value type info and write to buffer
			valueTypeInfo, _ := getActualTypeInfo(val, typeResolver)
			if err := typeResolver.writeTypeInfo(buf, valueTypeInfo); err != nil {
				return err
			}
			valueSerializer = valueTypeInfo.Serializer
		} else {
			chunkHeader |= VALUE_DECL_TYPE // Value type already declared
		}

		// Set tracking flags if reference tracking is enabled
		if f.referenceTracking {
			chunkHeader |= TRACKING_KEY_REF
		}
		if f.referenceTracking {
			chunkHeader |= TRACKING_VALUE_REF
		}

		// Write chunk header
		buf.PutUint8(chunkHeaderOffset, chunkHeader)
		chunkSize := 0

		// Serialize elements of same type in chunks
		keyType := getActualType(key)
		valueType := getActualType(val)
		for chunkSize < MAX_CHUNK_SIZE {
			if !isValid(key) || !isValid(val) || getActualType(key) != keyType || getActualType(val) != valueType {
				break
			}

			// Write key
			key = UnwrapReflectValue(key)
			if f.referenceTracking {
				if written, err := refResolver.WriteRefOrNull(buf, key); err != nil {
					return err
				} else if !written {
					if err := keySerializer.Write(f, buf, key); err != nil {
						return err
					}
				}
			} else {
				if err := keySerializer.Write(f, buf, key); err != nil {
					return err
				}
			}

			// Write value
			val = UnwrapReflectValue(val)
			if f.referenceTracking {
				if written, err := refResolver.WriteRefOrNull(buf, val); err != nil {
					return err
				} else if !written {
					if err := valueSerializer.Write(f, buf, val); err != nil {
						return err
					}
				}
			} else {
				if err := valueSerializer.Write(f, buf, val); err != nil {
					return err
				}
			}

			chunkSize++
			if iter.Next() {
				key, val = iter.Key(), iter.Value()
			} else {
				hasNext = false
				break
			}
		}

		// Reset serializers for next chunk
		keySerializer = nil
		valueSerializer = nil
		// Update chunk size in header
		buf.PutUint8(chunkHeaderOffset+1, uint8(chunkSize))
	}
	return nil
}

func (s mapSerializer) Read(f *Fory, buf *ByteBuffer, typ reflect.Type, value reflect.Value) error {
	// Initialize map if nil
	if value.IsNil() {
		value.Set(reflect.MakeMap(typ))
	}
	// Register reference for tracking
	f.refResolver.Reference(value)

	// Read map length from buffer
	length := buf.ReadVarUint32()

	var keySerializer Serializer
	var valueSerializer Serializer
	typeResolver := f.typeResolver
	refResolver := f.refResolver

	remaining := int(length)
	for remaining > 0 {
		header := buf.ReadUint8()

		// Handle special cases based on header flags
		switch {
		case header == (KEY_HAS_NULL | VALUE_HAS_NULL):
			// Null key and null value case
			value.SetMapIndex(reflect.Zero(typ.Key()), reflect.Zero(typ.Elem()))
			remaining--
			continue

		case (header & (KEY_HAS_NULL | VALUE_DECL_TYPE)) == (KEY_HAS_NULL | VALUE_DECL_TYPE):
			// Null key with declared value type case
			trackValueRef := (header & TRACKING_VALUE_REF) != 0
			var key, val reflect.Value

			key = reflect.Zero(typ.Key())
			if trackValueRef {
				// Handle reference tracking for value
				if refID, err := refResolver.TryPreserveRefId(buf); err != nil {
					return err
				} else if refID >= 0 {
					val = refResolver.GetReadObject(refID)
				} else {
					val = reflect.New(typ.Elem()).Elem()
					if err := valueSerializer.Read(f, buf, val.Type(), val); err != nil {
						return err
					}
					refResolver.SetReadObject(refID, val)
				}
			} else {
				// Read value without reference tracking
				val = reflect.New(typ.Elem()).Elem()
				if err := valueSerializer.Read(f, buf, val.Type(), val); err != nil {
					return err
				}
			}
			value.SetMapIndex(key, val)
			remaining--
			continue
		}

		// Chunk reading logic
		chunkHeader := header
		chunkSize := int(buf.ReadUint8())

		// Read type information if not declared
		if chunkHeader&KEY_DECL_TYPE == 0 {
			keyTypeInfo, err := typeResolver.readTypeInfo(buf)
			if err != nil {
				return err
			}
			keySerializer = keyTypeInfo.Serializer
		}
		if chunkHeader&VALUE_DECL_TYPE == 0 {
			valueTypeInfo, err := typeResolver.readTypeInfo(buf)
			if err != nil {
				return err
			}
			valueSerializer = valueTypeInfo.Serializer
		}

		// Check reference tracking flags
		trackKeyRef := chunkHeader&TRACKING_KEY_REF != 0
		trackValueRef := chunkHeader&TRACKING_VALUE_REF != 0

		// Process each element in the chunk
		for i := 0; i < chunkSize; i++ {
			if remaining <= 0 {
				return errors.New("invalid chunk size")
			}

			// Read key with or without reference tracking
			var key reflect.Value
			var refID int32
			if trackKeyRef {
				refID, _ = refResolver.TryPreserveRefId(buf)

				if int8(refID) < NotNullValueFlag {
					key = refResolver.GetCurrentReadObject()
				} else {
					key, _ = actualVal(typ.Key())
					if err := keySerializer.Read(f, buf, key.Type(), key); err != nil {
						return err
					}
					refResolver.SetReadObject(refID, key)
				}
			} else {
				key, _ = actualVal(typ.Key())
				if err := keySerializer.Read(f, buf, key.Type(), key); err != nil {
					return err
				}
			}

			// Read value with or without reference tracking
			var val reflect.Value
			if trackValueRef {
				refID, _ = refResolver.TryPreserveRefId(buf)

				if int8(refID) < NotNullValueFlag {
					val = refResolver.GetCurrentReadObject()
				} else {
					val, _ = actualVal(typ.Elem())
					if err := valueSerializer.Read(f, buf, val.Type(), val); err != nil {
						return err
					}
					refResolver.SetReadObject(refID, val)
				}
			} else {
				val, _ = actualVal(typ.Elem())
				if err := valueSerializer.Read(f, buf, val.Type(), val); err != nil {
					return err
				}
			}

			// Store key-value pair in map
			value.SetMapIndex(key, val)
			remaining--
		}
	}
	return nil
}

func getActualType(v reflect.Value) reflect.Type {
	if v.Kind() == reflect.Interface && !v.IsNil() {
		return v.Elem().Type()
	}
	return v.Type()
}

func getActualTypeInfo(v reflect.Value, resolver *typeResolver) (TypeInfo, error) {
	if v.Kind() == reflect.Interface && !v.IsNil() {
		elem := v.Elem()
		if !elem.IsValid() {
			return TypeInfo{}, fmt.Errorf("invalid interface value")
		}
		return resolver.getTypeInfo(elem, true)
	}
	return resolver.getTypeInfo(v, true)
}

func UnwrapReflectValue(v reflect.Value) reflect.Value {
	for v.Kind() == reflect.Interface && !v.IsNil() {
		v = v.Elem()
	}
	return v
}

func actualVal(t reflect.Type) (reflect.Value, error) {
	if t.Kind() == reflect.Interface {
		var container interface{}
		return reflect.ValueOf(&container).Elem(), nil
	}
	return reflect.New(t).Elem(), nil
}

func isValid(v reflect.Value) bool {
	return v.IsValid() && !v.IsZero()
}

type mapConcreteKeyValueSerializer struct {
	type_             reflect.Type
	keySerializer     Serializer
	valueSerializer   Serializer
	keyReferencable   bool
	valueReferencable bool
}

func (s *mapConcreteKeyValueSerializer) TypeId() TypeId {
	return -MAP
}

func (s *mapConcreteKeyValueSerializer) Write(f *Fory, buf *ByteBuffer, value reflect.Value) error {
	length := value.Len()
	if err := f.writeLength(buf, length); err != nil {
		return err
	}
	iter := value.MapRange()
	for iter.Next() {
		if s.keySerializer != nil {
			err := writeBySerializer(f, buf, iter.Key(), s.keySerializer, s.keyReferencable)
			if err != nil {
				return err
			}
		} else {
			if err := f.WriteReferencable(buf, iter.Key()); err != nil {
				return err
			}
		}
		if s.valueSerializer != nil {
			err := writeBySerializer(f, buf, iter.Value(), s.valueSerializer, s.valueReferencable)
			if err != nil {
				return err
			}
		} else {
			if err := f.WriteReferencable(buf, iter.Value()); err != nil {
				return err
			}
		}
	}
	return nil
}
func (s *mapConcreteKeyValueSerializer) Read(f *Fory, buf *ByteBuffer, type_ reflect.Type, value reflect.Value) error {
	if value.IsNil() {
		value.Set(reflect.MakeMap(s.type_))
	}
	f.refResolver.Reference(value)
	keyType := s.type_.Key()
	valueType := s.type_.Elem()
	length := f.readLength(buf)
	for i := 0; i < length; i++ {
		mapKey := reflect.New(keyType).Elem()
		if s.keySerializer != nil {
			if err := readBySerializer(f, buf, mapKey, s.keySerializer, s.keyReferencable); err != nil {
				return err
			}
		} else {
			if err := f.ReadReferencable(buf, mapKey); err != nil {
				return err
			}
		}
		mapValue := reflect.New(valueType).Elem()
		if s.valueSerializer != nil {
			if err := readBySerializer(f, buf, mapValue, s.valueSerializer, s.valueReferencable); err != nil {
				return err
			}
		} else {
			if err := f.ReadReferencable(buf, mapValue); err != nil {
				return err
			}
		}
		value.SetMapIndex(mapKey, mapValue)
	}
	return nil
}
