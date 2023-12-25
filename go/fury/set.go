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
	return FURY_SET
}

func (s setSerializer) Write(f *Fury, buf *ByteBuffer, value reflect.Value) error {
	mapData := value.Interface().(GenericSet)
	if err := f.writeLength(buf, len(mapData)); err != nil {
		return err
	}
	for k := range mapData {
		if err := f.WriteReferencable(buf, reflect.ValueOf(k)); err != nil {
			return err
		}
	}
	return nil
}

func (s setSerializer) Read(f *Fury, buf *ByteBuffer, type_ reflect.Type, value reflect.Value) error {
	if value.IsNil() {
		value.Set(reflect.ValueOf(GenericSet{}))
	}
	f.refResolver.Reference(value)
	genericSet := value.Interface().(GenericSet)
	length := f.readLength(buf)
	for i := 0; i < length; i++ {
		var mapKey interface{}
		if err := f.ReadReferencable(buf, reflect.ValueOf(&mapKey).Elem()); err != nil {
			return err
		}
		genericSet[mapKey] = true
	}
	return nil
}
