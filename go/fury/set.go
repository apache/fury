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

import "reflect"

// GenericSet type.
// TODO use golang generics; support more concrete key types
type GenericSet map[interface{}]bool

func (s GenericSet) Add(values ...interface{}) {
	for _, v := range values {
		s[v] = true
	}
}

type setSerializer struct {
}

func (s setSerializer) TypeId() TypeId {
	return SET
}

func (s setSerializer) Write(f *Fury, buf *ByteBuffer, value reflect.Value) error {
	keys := value.MapKeys()
	length := len(keys)
	if length == 0 {
		buf.WriteVarUint32(0)
		return nil
	}

	collectFlag, elemTypeInfo := s.writeHeader(f, buf, keys)

	if (collectFlag & CollectionNotSameType) == 0 {
		return s.writeSameType(f, buf, keys, elemTypeInfo, collectFlag)
	}
	return s.writeDifferentTypes(f, buf, keys)
}

func (s setSerializer) writeHeader(f *Fury, buf *ByteBuffer, keys []reflect.Value) (byte, TypeInfo) {
	collectFlag := CollectionDefaultFlag
	var elemTypeInfo TypeInfo
	hasNull := false
	hasDifferentType := false

	// 遍历元素检测类型
	// 初始化元素类型信息
	if len(keys) > 0 {
		firstElem := UnwrapReflectValue(keys[0])
		if isNull(firstElem) {
			hasNull = true
		} else {
			elemTypeInfo, _ = f.typeResolver.getTypeInfo(firstElem, true)
		}
	}

	// 遍历元素检测类型
	for _, key := range keys {
		key = UnwrapReflectValue(key)
		if isNull(key) {
			hasNull = true
			continue
		}

		currentTypeInfo, _ := f.typeResolver.getTypeInfo(key, true)
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
	buf.WriteVarUint32(uint32(len(keys)))
	buf.WriteInt8(int8(collectFlag))

	// 写入元素类型信息
	if !hasDifferentType {
		buf.WriteVarInt32(elemTypeInfo.TypeID)
	}

	return byte(collectFlag), elemTypeInfo
}

func (s setSerializer) writeSameType(f *Fury, buf *ByteBuffer, keys []reflect.Value, typeInfo TypeInfo, flag byte) error {
	serializer := typeInfo.Serializer
	trackRefs := (flag & CollectionTrackingRef) != 0

	for _, key := range keys {
		key = UnwrapReflectValue(key)
		if isNull(key) {
			buf.WriteInt8(NullFlag)
			continue
		}

		if trackRefs {
			refWritten, err := f.refResolver.WriteRefOrNull(buf, key)
			if err != nil {
				return err
			}
			if !refWritten {
				if err := serializer.Write(f, buf, key); err != nil {
					return err
				}
			}
		} else {
			if err := serializer.Write(f, buf, key); err != nil {
				return err
			}
		}
	}
	return nil
}

func (s setSerializer) writeDifferentTypes(f *Fury, buf *ByteBuffer, keys []reflect.Value) error {
	for _, key := range keys {
		key = UnwrapReflectValue(key)
		if isNull(key) {
			buf.WriteInt8(NullFlag)
			continue
		}

		typeInfo, _ := f.typeResolver.getTypeInfo(key, true)
		refWritten, err := f.refResolver.WriteRefOrNull(buf, key)
		if err != nil {
			return err
		}
		buf.WriteVarInt32(typeInfo.TypeID)
		if !refWritten {
			if err := typeInfo.Serializer.Write(f, buf, key); err != nil {
				return err
			}
		}
	}
	return nil
}

func (s setSerializer) Read(f *Fury, buf *ByteBuffer, type_ reflect.Type, value reflect.Value) error {
	length := int(buf.ReadVarUint32())
	if length == 0 {
		value.Set(reflect.MakeMap(type_))
		return nil
	}

	collectFlag := buf.ReadInt8()
	var elemTypeInfo TypeInfo

	if (collectFlag & CollectionNotSameType) == 0 {
		typeID := buf.ReadVarInt32()
		elemTypeInfo, _ = f.typeResolver.getTypeInfoById(int16(typeID))
	}

	if value.IsNil() {
		value.Set(reflect.MakeMap(type_))
	}
	f.refResolver.Reference(value)

	if (collectFlag & CollectionNotSameType) == 0 {
		return s.readSameType(f, buf, value, elemTypeInfo, collectFlag, length)
	}
	return s.readDifferentTypes(f, buf, value, length)
}

func (s setSerializer) readSameType(f *Fury, buf *ByteBuffer, value reflect.Value, typeInfo TypeInfo, flag int8, length int) error {
	trackRefs := (flag & CollectionTrackingRef) != 0
	serializer := typeInfo.Serializer

	for i := 0; i < length; i++ {
		var refID int32
		if trackRefs {
			refID, _ = f.refResolver.TryPreserveRefId(buf)
			if int8(refID) < NotNullValueFlag {
				elem := f.refResolver.GetReadObject(refID)
				value.SetMapIndex(reflect.ValueOf(elem), reflect.ValueOf(true))
				continue
			}
		}

		elem := reflect.New(typeInfo.Type).Elem()
		if err := serializer.Read(f, buf, elem.Type(), elem); err != nil {
			return err
		}

		if trackRefs {
			f.refResolver.SetReadObject(refID, elem)
		}
		value.SetMapIndex(elem, reflect.ValueOf(true))
	}
	return nil
}

func (s setSerializer) readDifferentTypes(f *Fury, buf *ByteBuffer, value reflect.Value, length int) error {
	for i := 0; i < length; i++ {
		refID, _ := f.refResolver.TryPreserveRefId(buf)
		typeID := buf.ReadVarInt32()
		typeInfo, _ := f.typeResolver.getTypeInfoById(int16(typeID))

		if int8(refID) < NotNullValueFlag {
			elem := f.refResolver.GetReadObject(refID)
			value.SetMapIndex(reflect.ValueOf(elem), reflect.ValueOf(true))
			continue
		}

		elem := reflect.New(typeInfo.Type).Elem()
		if err := typeInfo.Serializer.Read(f, buf, elem.Type(), elem); err != nil {
			return err
		}

		f.refResolver.SetReadObject(refID, elem)
		value.SetMapIndex(elem, reflect.ValueOf(true))
	}
	return nil
}
