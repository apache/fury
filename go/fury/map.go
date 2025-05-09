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

type mapSerializer struct {
}

func (s mapSerializer) TypeId() TypeId {
	return MAP
}

func (s mapSerializer) Write(f *Fury, buf *ByteBuffer, value reflect.Value) error {
	length := value.Len()
	if err := f.writeLength(buf, length); err != nil {
		return err
	}
	iter := value.MapRange()
	for iter.Next() {
		if err := f.WriteReferencable(buf, iter.Key()); err != nil {
			return err
		}
		if err := f.WriteReferencable(buf, iter.Value()); err != nil {
			return err
		}
	}
	return nil
}

func (s mapSerializer) Read(f *Fury, buf *ByteBuffer, type_ reflect.Type, value reflect.Value) error {
	if value.IsNil() {
		value.Set(reflect.MakeMap(type_))
	}
	f.refResolver.Reference(value)
	keyType := type_.Key()
	valueType := type_.Elem()
	length := f.readLength(buf)
	for i := 0; i < length; i++ {
		mapKey := reflect.New(keyType).Elem()
		if err := f.ReadReferencable(buf, mapKey); err != nil {
			return err
		}
		mapValue := reflect.New(valueType).Elem()
		if err := f.ReadReferencable(buf, mapValue); err != nil {
			return err
		}
		value.SetMapIndex(mapKey, mapValue)
	}
	return nil
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

func (s *mapConcreteKeyValueSerializer) Write(f *Fury, buf *ByteBuffer, value reflect.Value) error {
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
func (s *mapConcreteKeyValueSerializer) Read(f *Fury, buf *ByteBuffer, type_ reflect.Type, value reflect.Value) error {
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
