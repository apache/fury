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
	"github.com/stretchr/testify/require"
	"reflect"
	"testing"
	"unsafe"
)

func primitiveData() []interface{} {
	return []interface{}{
		false,
		true,
		byte(0),
		byte(1),
		byte(MaxUint8),
		int8(MinInt8),
		int8(-1),
		int8(1),
		int8(MaxInt8),
		int16(MinInt16),
		int16(-1),
		int16(1),
		int16(MaxInt16),
		int32(MinInt32),
		int32(-1),
		int32(1),
		int32(MaxInt32),
		int64(MinInt64),
		int64(-1),
		int64(1),
		int64(MaxInt64),
		MinInt,
		-1,
		1,
		MaxInt,
		float32(-1),
		float32(1),
		float64(-1),
		float64(1),
		"str",
		"",
	}
}

func commonSlice() []interface{} {
	return []interface{}{
		(&[100]bool{})[:],
		(&[100]byte{})[:],
		(&[100]int8{})[:],
		(&[100]int16{})[:],
		(&[100]int32{})[:],
		(&[100]int64{})[:],
		(&[100]float32{})[:],
		(&[100]float64{})[:],
		[]string{"str1", "str1", "", "", "str2"},
	}
}

func commonMap() []interface{} {
	return []interface{}{
		map[string]bool{"k1": false, "k2": true, "str": true, "": true},
		map[string]byte{"k1": 1, "k2": 1, "str": 2, "": 3},
		map[string]int8{"k1": 1, "k2": 1, "str": 2, "": 3},
		map[string]int16{"k1": 1, "k2": 1, "str": 2, "": 3},
		map[string]int32{"k1": 1, "k2": 1, "str": 2, "": 3},
		map[string]int64{"k1": 1, "k2": 1, "str": 2, "": 3},
		map[string]float32{"k1": 1, "k2": 1, "str": 2, "": 3},
		map[string]float64{"k1": 1, "k2": 1, "str": 2, "": 3},
		map[string]int32{"k1": 1, "k2": -1, "str": 2, "": 3},
		map[string]string{"k1": "v1", "k2": "v2", "str": "", "": ""},
		map[bool]bool{true: false, false: true},
		map[byte]byte{1: 1, 2: 2, 3: 3},
		map[int8]int8{1: 1, 2: 2, 3: 3},
		map[int16]int16{1: 1, 2: 2, 3: 3},
		map[int32]int32{1: 1, 2: 2, 3: 3},
		map[int64]int64{1: 1, 2: 2, 3: 3},
		map[float32]float32{1: 1, 2: 2, 3: 3},
		map[float64]float64{1: 1, 2: 2, 3: 3},
		map[interface{}]interface{}{"k1": "v1", "k2": "v2", "str": "", "": ""},
		map[string]interface{}{"k1": "v1", "k2": "v2", "str": "", "": ""},
		map[interface{}]string{"k1": "v1", "k2": "v2", "str": "", "": ""},
	}
}

func commonArray() []interface{} {
	return []interface{}{
		[100]bool{false, true, true},
		[100]byte{1, 2, 3},
		[100]int8{1, 2, 3},
		[100]int16{1, 2, 3},
		[100]int32{1, 2, 3},
		[100]int64{1, 2, 3},
		[100]float32{1, 2, 3},
		[100]float64{1, 2, 3},
		[100]string{"str1", "str1"},
	}
}

func TestSerializePrimitives(t *testing.T) {
	for _, referenceTracking := range []bool{false, true} {
		fory := NewFury(referenceTracking)
		for _, value := range primitiveData() {
			serde(t, fory, value)
		}
	}
}

func TestSerializeInterface(t *testing.T) {
	for _, referenceTracking := range []bool{false, true} {
		fory := NewFury(referenceTracking)
		var a interface{}
		a = -1
		serde(t, fory, a)
		b := []interface{}{1, 2, "str"}
		serde(t, fory, b)
		var newB []interface{}
		serDeserializeTo(t, fory, b, &newB)
		require.Equal(t, b, newB)
		// pointer to interface is not allowed.
		_, err := fory.Marshal(&a)
		require.Error(t, err)
	}
}

func TestSerializePtr(t *testing.T) {
	for _, referenceTracking := range []bool{false, true} {
		fory := NewFury(referenceTracking)
		a := -100
		b := &a
		serde(t, fory, b)
		x := "str"
		serde(t, fory, &x)
		x = ""
		serde(t, fory, &x)
		// pointer to pointer is not allowed.
		_, err := fory.Marshal(&b)
		require.Error(t, err)
	}
}

func TestSerializeSlice(t *testing.T) {
	for _, referenceTracking := range []bool{false, true} {
		fory := NewFury(referenceTracking)
		serde(t, fory, []byte{0, 1, MaxUint8})
		serde(t, fory, []int8{MinInt8, -1, 0, 1, MaxInt8})
		serde(t, fory, []int16{MinInt16, -1, 0, 1, MaxInt16})
		serde(t, fory, []int32{MinInt32, -1, 0, 1, MaxInt32})
		serde(t, fory, []int64{MinInt64, -1, 0, 1, MaxInt64})
		serde(t, fory, []float32{-1.0, 0, 1.0})
		serde(t, fory, []float64{-1.0, 0, 1.0})
		serde(t, fory, []string{"str1", "", "str2"})
		serde(t, fory, []interface{}{"", "", "str", "str"})
		serde(t, fory, primitiveData())
		for _, data := range commonSlice() {
			serde(t, fory, data)
		}
		serde(t, fory, commonSlice())
	}
}

func TestSerializeMap(t *testing.T) {
	for _, referenceTracking := range []bool{false, true} {
		fory := NewFury(referenceTracking)
		// "str1" is deserialized by interface type, which will be set to map key whose type is string.
		// so we need to save interface dynamic value type instead of interface value in reference resolver.
		{
			value := []interface{}{"str1", map[string]interface{}{"str1": "str2"}}
			serde(t, fory, value)
		}
		{
			value := map[string]interface{}{"k1": "v1", "str": "", "": ""}
			serde(t, fory, value)
		}
		{
			value := map[string]int32{
				"k1": MinInt32,
				"k2": 0,
				"k3": MaxInt32,
			}
			serde(t, fory, value)
		}
		for _, data := range commonMap() {
			serde(t, fory, data)
		}
		serde(t, fory, commonMap())
	}
}

func TestSerializeArray(t *testing.T) {
	for _, referenceTracking := range []bool{false, true} {
		fory := NewFury(referenceTracking)
		for _, data := range commonArray() {
			serde(t, fory, data)
		}
		serde(t, fory, commonArray())
	}
}

func TestSerializeStructSimple(t *testing.T) {
	for _, referenceTracking := range []bool{false, true} {
		fory := NewFury(referenceTracking)
		type A struct {
			F1 []string
		}
		require.Nil(t, fory.RegisterTagType("example.A", A{}))
		serde(t, fory, A{})
		serde(t, fory, &A{})
		serde(t, fory, A{F1: []string{"str1", "", "str2"}})
		serde(t, fory, &A{F1: []string{"str1", "", "str2"}})

		type B struct {
			F1 []string
			F2 map[string]int32
		}
		require.Nil(t, fory.RegisterTagType("example.B", B{}))
		serde(t, fory, B{})
		serde(t, fory, B{
			F1: []string{"str1", "", "str2"},
			F2: map[string]int32{
				"k1": 1,
				"k2": 2,
			},
		})
	}
}

func TestSerializeBeginWithMagicNumber(t *testing.T) {
	strSlice := []string{"str1", "str1", "", "", "str2"}
	fory := NewFury(true)
	bytes, err := fory.Marshal(strSlice)
	require.Nil(t, err, fmt.Sprintf("serialize value %s with type %s failed: %s",
		reflect.ValueOf(strSlice), reflect.TypeOf(strSlice), err))
	// Contains at least two bytes.
	require.True(t, len(bytes) > 2)
	magicNumber := int16(bytes[0]) | (int16(bytes[1]) << 8)
	require.Equal(t, magicNumber, MAGIC_NUMBER)
}

type Foo struct {
	F1 int32
	F2 string
	F3 []string
	F4 map[string]int32
	F5 Bar
}

type Bar struct {
	F1 int32
	F2 string
}

func newFoo() Foo {
	return Foo{
		F1: 1,
		F2: "str",
		F3: []string{"str1", "", "str2"},
		F4: map[string]int32{
			"k1": 1,
			"k2": 2,
			"k3": 3,
			"k4": 4,
			"k5": 5,
			"k6": 6,
		},
		F5: Bar{
			F1: 1,
			F2: "str",
		},
	}
}

func TestSerializeStruct(t *testing.T) {
	for _, referenceTracking := range []bool{false, true} {
		fory := NewFury(referenceTracking)
		require.Nil(t, fory.RegisterTagType("example.Bar", Bar{}))
		serde(t, fory, &Bar{})
		bar := Bar{F1: 1, F2: "str"}
		serde(t, fory, bar)
		serde(t, fory, &bar)

		type A struct {
			F1 Bar
			F2 interface{}
		}
		require.Nil(t, fory.RegisterTagType("example.A", A{}))
		serde(t, fory, A{})
		serde(t, fory, &A{})
		serde(t, fory, A{F1: Bar{F1: 1, F2: "str"}, F2: -1})
		serde(t, fory, &A{F1: Bar{F1: 1, F2: "str"}, F2: -1})

		require.Nil(t, fory.RegisterTagType("example.Foo", Foo{}))
		foo := newFoo()
		serde(t, fory, foo)
		serde(t, fory, &foo)
	}
}

func TestSerializeStringReference(t *testing.T) {
	fory := NewFury(true)
	strSlice := []string{"str1", "str1", "", "", "str2"}
	strSlice = append(strSlice, strSlice[0])
	serde(t, fory, strSlice)
	type A struct {
		F1 string
		F2 string
	}
	require.Nil(t, fory.RegisterTagType("example.A", A{}))
	serde(t, fory, A{})
	serde(t, fory, A{F1: "str", F2: "str"})
	var strData []byte
	for i := 0; i < 1000; i++ {
		strData = append(strData, 100)
	}
	x := string(strData)
	serde(t, fory, &x)
	strSlice2 := []string{x, x, x}
	bytes, err := fory.Marshal(strSlice2)
	require.Nil(t, err)
	require.Less(t, len(bytes), 2*len(strData))
	strSlice23 := []*string{&x, &x, &x}
	bytes, err = fory.Marshal(strSlice23)
	require.Nil(t, err)
	require.Less(t, len(bytes), 2*len(strData))
}

func TestSerializeCircularReference(t *testing.T) {
	fory := NewFury(true)
	{
		type A struct {
			A1 *A
		}
		require.Nil(t, fory.RegisterTagType("example.A", A{}))
		// If use `A{}` instead of `&A{}` and pass `a` instead of `&a`, there will be serialization data duplication
		// and can't be deserialized by other languages too.
		// TODO(chaokunyang) If pass by value(have a copy) and there are some inner value reference, return a readable
		//  error instead of panic on `Unmarshal`
		a := &A{}
		a.A1 = a
		bytes, err := fory.Marshal(a)
		require.Nil(t, err)
		var a1 *A
		err = fory.Unmarshal(bytes, &a1)
		require.Nil(t, err)
		require.Same(t, a1, a1.A1)
	}
	{
		type B struct {
			F1 string
			F2 *B
			F3 *B
		}
		require.Nil(t, fory.RegisterTagType("example.B", B{}))
		b := &B{F1: "str"}
		b.F2 = b
		b.F3 = b
		bytes, err := fory.Marshal(b)
		require.Nil(t, err)
		var b1 *B
		err = fory.Unmarshal(bytes, &b1)
		require.Nil(t, err)
		require.Equal(t, b.F1, b1.F1)
		require.Same(t, b1, b1.F2)
		require.Same(t, b1.F2, b1.F3)
	}
}

func TestSerializeComplexReference(t *testing.T) {
	fory := NewFury(true)
	type A struct {
		F1 string
		F2 *A
		F3 *A
	}
	type B struct {
		F1 []string
		F2 map[string]int32
		F3 *A
		F4 *B
	}
	require.Nil(t, fory.RegisterTagType("example.A", A{}))
	require.Nil(t, fory.RegisterTagType("example.B", B{}))

	a := &A{F1: "str"}
	a.F2 = a
	a.F3 = a
	b := &B{
		F1: []string{"str1", "str1", "", "", "str2"},
		F2: map[string]int32{"k1": 1, "k2": -1, "str": 2, "": 3},
	}
	b.F3 = a
	b.F4 = b
	value := []*B{b, b}

	bytes, err := fory.Marshal(value)
	require.Nil(t, err)
	var b1 []*B
	err = fory.Unmarshal(bytes, &b1)
	require.Nil(t, err)
	require.Same(t, b1[0], b1[1])
	require.Same(t, b1[0], b1[0].F4)
	require.Same(t, b1[0].F3, b1[0].F3.F2)
	require.Same(t, b1[0].F3, b1[0].F3.F3)
	require.Equal(t, b1[0].F1, b1[1].F1)
	require.Equal(t, b1[0].F2, b1[1].F2)
}

func TestSerializeCommonReference(t *testing.T) {
	fory := NewFury(true)
	var values []interface{}
	values = append(values, commonSlice()...)
	values = append(values, commonMap()...)
	for _, data := range values {
		value := []interface{}{data, data}
		bytes, err := fory.Marshal(value)
		require.Nil(t, err)
		var newValue []interface{}
		require.Nil(t, fory.Unmarshal(bytes, &newValue))
		require.Equal(t, unsafe.Pointer(reflect.ValueOf(newValue[0]).Pointer()),
			unsafe.Pointer(reflect.ValueOf(newValue[1]).Pointer()))
		require.Equal(t, newValue[0], newValue[1])
	}
}

func TestSerializeZeroCopy(t *testing.T) {
	fory := NewFury(true)
	list := []interface{}{"str", make([]byte, 1000)}
	buf := NewByteBuffer(nil)
	var bufferObjects []BufferObject
	require.Nil(t, fory.Serialize(buf, list, func(o BufferObject) bool {
		bufferObjects = append(bufferObjects, o)
		return false
	}))
	require.Equal(t, 1, len(bufferObjects))
	var newList []interface{}
	var buffers []*ByteBuffer
	for _, o := range bufferObjects {
		buffers = append(buffers, o.ToBuffer())
	}
	err := fory.Deserialize(buf, &newList, buffers)
	require.Nil(t, err)
	require.Equal(t, list, newList)
}

func serDeserializeTo(t *testing.T, fory *Fory, value interface{}, to interface{}) {
	bytes, err := fory.Marshal(value)
	require.Nil(t, err, fmt.Sprintf("serialize value %s with type %s failed: %s",
		reflect.ValueOf(value), reflect.TypeOf(value), err))
	require.Nil(t, fory.Unmarshal(bytes, to),
		fmt.Sprintf("deserialize value %s with type %s failed: %s",
			reflect.ValueOf(value), reflect.TypeOf(value), err))
	require.Equal(t, value, reflect.ValueOf(to).Elem().Interface())
}

func serde(t *testing.T, fory *Fory, value interface{}) {
	bytes, err := fory.Marshal(value)
	require.Nil(t, err, fmt.Sprintf("serialize value %s with type %s failed: %s",
		reflect.ValueOf(value), reflect.TypeOf(value), err))
	var newValue interface{}
	require.Nil(t, fory.Unmarshal(bytes, &newValue),
		fmt.Sprintf("deserialize value %s with type %s failed: %s",
			reflect.ValueOf(value), reflect.TypeOf(value), err))
	if reflect.ValueOf(value).Kind() == reflect.Ptr {
		require.Equal(t, reflect.ValueOf(value).Elem().Interface(),
			reflect.ValueOf(newValue).Elem().Interface())
	} else {
		require.Equal(t, value, newValue)
	}
}

// cover:
//  go test -cover
// benchmark:
//  go test -bench=. -benchmem
// profile:
//  go test -bench=BenchmarkMarshal -cpuprofile=cpu.log
//  go tool pprof -text -nodecount=10 ./fory.test cpu.log
//  go test -bench=BenchmarkMarshal -memprofile=mem.out
//  go tool pprof -text -nodecount=10 ./fory.test mem.out

func BenchmarkMarshal(b *testing.B) {
	fory := NewFury(true)
	require.Nil(b, fory.RegisterTagType("example.Foo", Foo{}))
	require.Nil(b, fory.RegisterTagType("example.Bar", Bar{}))
	value := benchData()
	for i := 0; i < b.N; i++ {
		_, err := fory.Marshal(value)
		if err != nil {
			panic(err)
		}
	}
}

func BenchmarkUnmarshal(b *testing.B) {
	fory := NewFury(true)
	require.Nil(b, fory.RegisterTagType("example.Foo", Foo{}))
	require.Nil(b, fory.RegisterTagType("example.Bar", Bar{}))
	value := benchData()
	data, err := fory.Marshal(value)
	if err != nil {
		panic(err)
	}
	for i := 0; i < b.N; i++ {
		var newFoo interface{}
		err := fory.Unmarshal(data, &newFoo)
		if err != nil {
			panic(err)
		}
	}
}

func benchData() interface{} {
	var strData []byte
	for i := 0; i < 1000; i++ {
		strData = append(strData, 100)
	}
	x := string(strData)
	return []string{x, x, x, x}
}

func ExampleMarshal() {
	list := []interface{}{true, false, "str", -1.1, 1, make([]int32, 5), make([]float64, 5)}
	bytes, err := Marshal(list)
	if err != nil {
		panic(err)
	}
	var newValue interface{}
	// bytes can be data serialized by other languages.
	if err := Unmarshal(bytes, &newValue); err != nil {
		panic(err)
	}
	fmt.Println(newValue)

	dict := map[string]interface{}{
		"k1": "v1",
		"k2": list,
		"k3": -1,
	}
	bytes, err = Marshal(dict)
	if err != nil {
		panic(err)
	}
	// bytes can be data serialized by other languages.
	if err := Unmarshal(bytes, &newValue); err != nil {
		panic(err)
	}
	fmt.Println(newValue)
	// Output:
	// [true false str -1.1 1 [0 0 0 0 0] [0 0 0 0 0]]
	// map[k1:v1 k2:[true false str -1.1 1 [0 0 0 0 0] [0 0 0 0 0]] k3:-1]
}
