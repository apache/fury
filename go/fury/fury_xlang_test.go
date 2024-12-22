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

//go:build skiptest
// +build skiptest

package fury_test

import (
	"fmt"
	"github.com/apache/fury/go/fury"
	"github.com/stretchr/testify/require"
	"io/ioutil"
	"os"
	"os/exec"
	"reflect"
	"testing"
	"time"
	"unsafe"
)

const pythonModule = "pyfury.tests.test_cross_language"

func executeCommand(commandStr []string) bool {
	fmt.Printf("Executing command %s\n", commandStr)
	cmd := exec.Command(commandStr[0], commandStr[1:]...)
	output, err := cmd.CombinedOutput()
	if err != nil {
		fmt.Println(fmt.Sprint(err) + ": " + string(output))
		return false
	}
	fmt.Println(string(output))
	return true
}

func TestBuffer(t *testing.T) {
	buffer := fury.NewByteBuffer(nil)
	buffer.WriteBool(true)
	buffer.WriteByte_(fury.MaxInt8)
	buffer.WriteInt16(fury.MaxInt16)
	buffer.WriteInt32(fury.MaxInt32)
	buffer.WriteInt64(fury.MaxInt64)
	buffer.WriteFloat32(-1.1)
	buffer.WriteFloat64(-1.1)
	buffer.WriteVarInt32(100)
	bytes := []byte{'a', 'b'}
	buffer.WriteInt32(int32(len(bytes)))
	buffer.WriteBinary(bytes)
	require.Nil(t, ioutil.WriteFile("test_buffer.data", buffer.GetByteSlice(0, buffer.WriterIndex()), 0644))
	defer func(name string) {
		err := os.Remove(name)
		require.Nil(t, err)
	}("test_buffer.data")
	require.True(t, executeCommand([]string{"python", "-m", pythonModule, "test_buffer", "test_buffer.data"}))

	data, err := ioutil.ReadFile("test_buffer.data")
	require.Nil(t, err)
	buffer = fury.NewByteBuffer(data)
	require.True(t, buffer.ReadBool())
	require.Equal(t, buffer.ReadByte_(), byte(fury.MaxInt8))
	require.Equal(t, buffer.ReadInt16(), int16(fury.MaxInt16))
	require.Equal(t, buffer.ReadInt32(), int32(fury.MaxInt32))
	require.Equal(t, buffer.ReadInt64(), int64(fury.MaxInt64))
	require.Equal(t, buffer.ReadFloat32(), float32(-1.1))
	require.Equal(t, buffer.ReadFloat64(), -1.1)
	require.Equal(t, buffer.ReadVarInt32(), int32(100))
	require.Equal(t, buffer.ReadBinary(int(buffer.ReadInt32())), bytes)
}

func TestXLangSerializer(t *testing.T) {
	fury_ := fury.NewFury(true)
	buffer := fury.NewByteBuffer(nil)
	require.Nil(t, fury_.Serialize(buffer, true, nil))
	require.Nil(t, fury_.Serialize(buffer, false, nil))
	require.Nil(t, fury_.Serialize(buffer, int64(-1), nil))
	require.Nil(t, fury_.Serialize(buffer, int8(fury.MaxInt8), nil))
	require.Nil(t, fury_.Serialize(buffer, int8(fury.MinInt8), nil))
	require.Nil(t, fury_.Serialize(buffer, int16(fury.MaxInt16), nil))
	require.Nil(t, fury_.Serialize(buffer, int16(fury.MinInt16), nil))
	require.Nil(t, fury_.Serialize(buffer, int32(fury.MaxInt32), nil))
	require.Nil(t, fury_.Serialize(buffer, int32(fury.MinInt32), nil))
	require.Nil(t, fury_.Serialize(buffer, int64(fury.MaxInt64), nil))
	require.Nil(t, fury_.Serialize(buffer, int64(fury.MinInt64), nil))
	require.Nil(t, fury_.Serialize(buffer, float32(-1), nil))
	require.Nil(t, fury_.Serialize(buffer, float64(-1), nil))
	require.Nil(t, fury_.Serialize(buffer, "str", nil))
	day := fury.Date{2021, 11, 23}
	instant := time.Unix(100, 0)
	require.Nil(t, fury_.Serialize(buffer, day, nil))
	require.Nil(t, fury_.Serialize(buffer, instant, nil))
	list := []interface{}{"a", int64(1), -1.0, instant, day}
	require.Nil(t, fury_.Serialize(buffer, list, nil))
	dict := map[interface{}]interface{}{}
	for index, item := range list {
		dict[fmt.Sprintf("k%d", index)] = item
		dict[item] = item
	}
	require.Nil(t, fury_.Serialize(buffer, dict, nil))
	set := fury.GenericSet{}
	set.Add(list...)
	require.Nil(t, fury_.Serialize(buffer, set, nil))

	// test primitive arrays
	require.Nil(t, fury_.Serialize(buffer, []bool{true, false}, nil))
	require.Nil(t, fury_.Serialize(buffer, []int16{1, fury.MaxInt16}, nil))
	require.Nil(t, fury_.Serialize(buffer, []int32{1, fury.MaxInt32}, nil))
	require.Nil(t, fury_.Serialize(buffer, []int64{1, fury.MaxInt64}, nil))
	require.Nil(t, fury_.Serialize(buffer, []float32{1.0, 2.0}, nil))
	require.Nil(t, fury_.Serialize(buffer, []float64{1.0, 2.0}, nil))

	check := func(buf *fury.ByteBuffer) {
		values := []interface{}{
			true, false, int64(-1), int8(fury.MaxInt8), int8(fury.MinInt8), int16(fury.MaxInt16), int16(fury.MinInt16),
			int32(fury.MaxInt32), int32(fury.MinInt32), int64(fury.MaxInt64), int64(fury.MinInt64), float32(-1),
			float64(-1), "str", day, instant, list, dict, set,
			[]bool{true, false}, []int16{1, fury.MaxInt16}, []int32{1, fury.MaxInt32},
			[]int64{1, fury.MaxInt64}, []float32{1.0, 2.0}, []float64{1.0, 2.0},
		}
		for index, value := range values {
			var newValue interface{}
			require.Nil(t, fury_.Deserialize(buf, &newValue, nil))
			switch reflect.ValueOf(value).Kind() {
			case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64:
				require.Equal(t, reflect.ValueOf(value).Int(),
					reflect.ValueOf(newValue).Int(), fmt.Sprintf("index %d", index))
			case reflect.Float32, reflect.Float64:
				require.Equal(t, reflect.ValueOf(value).Float(),
					reflect.ValueOf(newValue).Float(), fmt.Sprintf("index %d", index))
			default:
				require.Equal(t, value, newValue, fmt.Sprintf("index %d", index))
			}
		}
	}
	check(buffer)

	require.Nil(t, ioutil.WriteFile("test_cross_language_serializer.data",
		buffer.GetByteSlice(0, buffer.WriterIndex()), 0644))
	defer func(name string) {
		err := os.Remove(name)
		require.Nil(t, err)
	}("test_cross_language_serializer.data")
	require.True(t, executeCommand([]string{"python", "-m", pythonModule,
		"test_cross_language_serializer", "test_cross_language_serializer.data"}))

	data, err := ioutil.ReadFile("test_cross_language_serializer.data")
	require.Nil(t, err)
	buffer = fury.NewByteBuffer(data)
	check(buffer)
}

func TestCrossLanguageReference(t *testing.T) {
	fury_ := fury.NewFury(true)
	list := make([]interface{}, 2, 2)
	dict := map[interface{}]interface{}{}
	list[0] = list
	list[1] = dict
	dict["k1"] = dict
	dict["k2"] = list
	marshal, err := fury_.Marshal(list)
	require.Nil(t, err)
	check := func(dataBytes []byte) {
		var newList []interface{}
		require.Nil(t, fury_.Unmarshal(dataBytes, &newList))
		require.True(t, same(newList, newList[0]))
		newMap := newList[1].(map[interface{}]interface{})
		require.True(t, same(newMap["k1"], newMap))
		require.True(t, same(newMap["k2"], newList))
	}
	check(marshal)

	require.Nil(t, ioutil.WriteFile("test_cross_language_reference.data", marshal, 0644))
	defer func(name string) {
		err := os.Remove(name)
		require.Nil(t, err)
	}("test_cross_language_reference.data")
	require.True(t, executeCommand([]string{"python", "-m", pythonModule,
		"test_cross_language_reference", "test_cross_language_reference.data"}))
	data, err := ioutil.ReadFile("test_cross_language_reference.data")
	require.Nil(t, err)
	check(data)
}

func same(x, y interface{}) bool {
	var vx, vy = reflect.ValueOf(x), reflect.ValueOf(y)
	if vx.Type() != vy.Type() {
		return false
	}
	if vx.Type().Kind() == reflect.Slice {
		if vx.Len() != vy.Len() {
			return false
		}
	}
	return unsafe.Pointer(reflect.ValueOf(x).Pointer()) == unsafe.Pointer(reflect.ValueOf(y).Pointer())
}

type ComplexObject1 struct {
	F1  interface{}
	F2  string
	F3  []interface{}
	F4  map[int8]int32
	F5  int8
	F6  int16
	F7  int32
	F8  int64
	F9  float32
	F10 float64
	F11 []int16
	F12 fury.Int16Slice
}

type ComplexObject2 struct {
	F1 interface{}
	F2 map[int8]int32
}

func TestSerializeSimpleStruct(t *testing.T) {
	fury_ := fury.NewFury(true)
	require.Nil(t, fury_.RegisterTagType("test.ComplexObject2", ComplexObject2{}))
	obj2 := &ComplexObject2{}
	obj2.F1 = true
	obj2.F2 = map[int8]int32{-1: 2}
	structRoundBack(t, fury_, obj2, "test_serialize_simple_struct")
}

func TestSerializeComplexStruct(t *testing.T) {
	fury_ := fury.NewFury(true)
	require.Nil(t, fury_.RegisterTagType("test.ComplexObject1", ComplexObject1{}))
	require.Nil(t, fury_.RegisterTagType("test.ComplexObject2", ComplexObject2{}))
	obj2 := &ComplexObject2{}
	obj2.F1 = true
	obj2.F2 = map[int8]int32{-1: 2}

	obj := &ComplexObject1{}
	obj.F1 = obj2
	obj.F2 = "abc"
	obj.F3 = []interface{}{"abc", "abc"}
	f4 := map[int8]int32{1: 2}
	obj.F4 = f4
	obj.F5 = fury.MaxInt8
	obj.F6 = fury.MaxInt16
	obj.F7 = fury.MaxInt32
	obj.F8 = fury.MaxInt64
	obj.F9 = 1.0 / 2
	obj.F10 = 1 / 3.0
	obj.F11 = []int16{1, 2}
	obj.F12 = []int16{-1, 4}

	structRoundBack(t, fury_, obj, "test_serialize_complex_struct")
}

func structRoundBack(t *testing.T, fury_ *fury.Fury, obj interface{}, testName string) {
	check := func(data []byte) {
		var newObj interface{}
		require.Nil(t, fury_.Unmarshal(data, &newObj))
		require.Equal(t, obj, newObj)
	}
	marshal, err := fury_.Marshal(obj)
	require.Nil(t, err)
	check(marshal)

	require.Nil(t, ioutil.WriteFile(testName, marshal, 0644))
	defer func(name string) {
		err := os.Remove(name)
		require.Nil(t, err)
	}(testName)
	fmt.Println(os.Getwd())
	require.True(t, executeCommand([]string{"python", "-m", pythonModule,
		testName, testName}))
	data, err := ioutil.ReadFile(testName)
	require.Nil(t, err)
	check(data)
}

func TestOutOfBandBuffer(t *testing.T) {
	fury_ := fury.NewFury(true)
	var data [][]byte
	for i := 0; i < 10; i++ {
		data = append(data, []byte{0, 1})
	}
	var bufferObjects []fury.BufferObject
	buffer := fury.NewByteBuffer(nil)
	counter := 0
	err := fury_.Serialize(buffer, data, func(o fury.BufferObject) bool {
		counter += 1
		if counter%2 == 0 {
			bufferObjects = append(bufferObjects, o)
			return false
		} else {
			return true
		}
	})
	require.Nil(t, err)
	var buffers []*fury.ByteBuffer
	for _, object := range bufferObjects {
		buffers = append(buffers, object.ToBuffer())
	}
	var newObj interface{}
	err = fury_.Deserialize(buffer, &newObj, buffers)
	require.Nil(t, err)
	require.Equal(t, newObj, data)

	buffer.SetReaderIndex(0)
	require.Nil(t, ioutil.WriteFile("test_oob_buffer_in_band.data",
		buffer.GetByteSlice(0, buffer.WriterIndex()), 0644))
	fmt.Printf("in band size: %d\n", buffer.WriterIndex())
	defer func(name string) {
		err := os.Remove(name)
		require.Nil(t, err)
	}("test_oob_buffer_in_band.data")
	outOfBandBuffer := fury.NewByteBuffer(nil)
	outOfBandBuffer.WriteInt32(int32(len(buffers)))
	for _, bufferObject := range bufferObjects {
		outOfBandBuffer.WriteInt32(int32(bufferObject.TotalBytes()))
		bufferObject.WriteTo(outOfBandBuffer)
	}
	require.Nil(t, ioutil.WriteFile("test_oob_buffer_out_of_band.data",
		outOfBandBuffer.GetByteSlice(0, outOfBandBuffer.WriterIndex()), 0644))
	defer func(name string) {
		err := os.Remove(name)
		require.Nil(t, err)
	}("test_oob_buffer_out_of_band.data")

	fmt.Println(os.Getwd())
	require.True(t, executeCommand([]string{"python", "-m", pythonModule,
		"test_oob_buffer", "test_oob_buffer_in_band.data", "test_oob_buffer_out_of_band.data"}))

	inBandData, err1 := ioutil.ReadFile("test_oob_buffer_in_band.data")
	inBandDataBuffer := fury.NewByteBuffer(inBandData)
	require.Nil(t, err1)
	outOfBandData, err2 := ioutil.ReadFile("test_oob_buffer_out_of_band.data")
	require.Nil(t, err2)
	outOfBandBuffer = fury.NewByteBuffer(outOfBandData)
	buffers = []*fury.ByteBuffer{}
	numBuffer := outOfBandBuffer.ReadInt32()
	for i := 0; i < int(numBuffer); i++ {
		length := int(outOfBandBuffer.ReadInt32())
		readerIndex := outOfBandBuffer.ReaderIndex()
		buffers = append(buffers, outOfBandBuffer.Slice(readerIndex, length))
		outOfBandBuffer.SetReaderIndex(readerIndex + length)
	}
	var newObj2 [][]byte
	err = fury_.Deserialize(inBandDataBuffer, &newObj2, buffers)
	require.Nil(t, err)
	require.Equal(t, data, newObj2)
}
