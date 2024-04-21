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
	"io"
)

type ByteBuffer struct {
	io.Writer
	io.Reader
	writerIndex int
	readerIndex int
	data        []byte
}

func NewByteBuffer(data []byte) *ByteBuffer {
	return &ByteBuffer{data: data}
}

func (b *ByteBuffer) grow(n int) {
	l := b.writerIndex
	if l+n < len(b.data) {
		return
	}
	if l+n < cap(b.data) {
		b.data = b.data[:cap(b.data)]
	} else {
		newBuf := make([]byte, 2*(l+n), 2*(l+n))
		copy(newBuf, b.data)
		b.data = newBuf
	}
}

func (b *ByteBuffer) WriteBool(value bool) {
	b.grow(1)
	if value {
		b.data[b.writerIndex] = 1
	} else {
		b.data[b.writerIndex] = 0
	}
	b.writerIndex++
}

func (b *ByteBuffer) WriteByte(value byte) error {
	b.grow(1)
	b.data[b.writerIndex] = value
	b.writerIndex++
	return nil
}

func (b *ByteBuffer) WriteByte_(value byte) {
	b.grow(1)
	b.data[b.writerIndex] = value
	b.writerIndex++
}

func (b *ByteBuffer) WriteInt8(value int8) {
	b.grow(1)
	b.data[b.writerIndex] = byte(value)
	b.writerIndex++
}

func (b *ByteBuffer) WriteInt16(value int16) {
	b.grow(2)
	binary.LittleEndian.PutUint16(b.data[b.writerIndex:], uint16(value))
	b.writerIndex += 2
}

func (b *ByteBuffer) WriteInt32(value int32) {
	b.grow(4)
	binary.LittleEndian.PutUint32(b.data[b.writerIndex:], uint32(value))
	b.writerIndex += 4
}

func (b *ByteBuffer) WriteLength(value int) {
	b.grow(4)
	if value >= MaxInt32 {
		panic(fmt.Errorf("too long: %d", value))
	}
	b.WriteVarInt32(int32(value))
}

func (b *ByteBuffer) ReadLength() int {
	return int(b.ReadVarInt32())
}

func (b *ByteBuffer) WriteInt64(value int64) {
	b.grow(8)
	binary.LittleEndian.PutUint64(b.data[b.writerIndex:], uint64(value))
	b.writerIndex += 8
}

func (b *ByteBuffer) WriteFloat32(value float32) {
	b.grow(4)
	binary.LittleEndian.PutUint32(b.data[b.writerIndex:], Float32bits(value))
	b.writerIndex += 4
}

func (b *ByteBuffer) WriteFloat64(value float64) {
	b.grow(8)
	binary.LittleEndian.PutUint64(b.data[b.writerIndex:], Float64bits(value))
	b.writerIndex += 8
}

func (b *ByteBuffer) Write(p []byte) (n int, err error) {
	b.grow(len(p))
	l := copy(b.data[b.writerIndex:], p)
	b.writerIndex += len(p)
	return l, nil
}

func (b *ByteBuffer) WriteBinary(p []byte) {
	b.grow(len(p))
	l := copy(b.data[b.writerIndex:], p)
	if l != len(p) {
		panic(fmt.Errorf("should write %d bytes, but written %d bytes", len(p), l))
	}
	b.writerIndex += len(p)
}

func (b *ByteBuffer) ReadBool() bool {
	v := b.data[b.readerIndex]
	b.readerIndex++
	if v == 0 {
		return false
	} else {
		return true
	}
}

func (b *ByteBuffer) ReadByte_() byte {
	v := b.data[b.readerIndex]
	b.readerIndex++
	return v
}

func (b *ByteBuffer) ReadByte() (byte, error) {
	v := b.data[b.readerIndex]
	b.readerIndex++
	return v, nil
}

func (b *ByteBuffer) ReadInt8() int8 {
	i := int8(b.data[b.readerIndex])
	b.readerIndex += 1
	return i
}

func (b *ByteBuffer) ReadInt16() int16 {
	i := int16(binary.LittleEndian.Uint16(b.data[b.readerIndex:]))
	b.readerIndex += 2
	return i
}

func (b *ByteBuffer) ReadUint32() uint32 {
	i := binary.LittleEndian.Uint32(b.data[b.readerIndex:])
	b.readerIndex += 4
	return i
}

func (b *ByteBuffer) ReadUint64() uint64 {
	i := binary.LittleEndian.Uint64(b.data[b.readerIndex:])
	b.readerIndex += 8
	return i
}

func (b *ByteBuffer) ReadInt32() int32 {
	return int32(b.ReadUint32())
}

func (b *ByteBuffer) ReadInt64() int64 {
	return int64(b.ReadUint64())
}

func (b *ByteBuffer) ReadFloat32() float32 {
	return Float32frombits(b.ReadUint32())
}

func (b *ByteBuffer) ReadFloat64() float64 {
	return Float64frombits(b.ReadUint64())
}

func (b *ByteBuffer) Read(p []byte) (n int, err error) {
	copied := copy(p, b.data[b.readerIndex:])
	b.readerIndex += copied
	return copied, nil
}

func (b *ByteBuffer) ReadBinary(length int) []byte {
	v := b.data[b.readerIndex : b.readerIndex+length]
	b.readerIndex += length
	return v
}

func (b *ByteBuffer) GetData() []byte {
	return b.data
}

func (b *ByteBuffer) GetByteSlice(start, end int) []byte {
	return b.data[start:end]
}

func (b *ByteBuffer) Slice(start, length int) *ByteBuffer {
	return NewByteBuffer(b.data[start : start+length])
}

func (b *ByteBuffer) WriterIndex() int {
	return b.writerIndex
}

func (b *ByteBuffer) SetWriterIndex(index int) {
	b.writerIndex = index
}

func (b *ByteBuffer) ReaderIndex() int {
	return b.readerIndex
}

func (b *ByteBuffer) SetReaderIndex(index int) {
	b.readerIndex = index
}

func (b *ByteBuffer) Reset() {
	b.readerIndex = 0
	b.writerIndex = 0
	b.data = nil
}

func (b *ByteBuffer) PutInt32(index int, value int32) {
	b.grow(4)
	binary.LittleEndian.PutUint32(b.data[index:], uint32(value))
}

// WriteVarInt32 WriteVarUint writes a 1-5 byte int, returns the number of bytes written.
func (b *ByteBuffer) WriteVarInt32(value int32) int8 {
	if value>>7 == 0 {
		b.grow(1)
		b.data[b.writerIndex] = byte(value)
		b.writerIndex++
		return 1
	}
	if value>>14 == 0 {
		b.grow(2)
		b.data[b.writerIndex] = byte((value & 0x7F) | 0x80)
		b.data[b.writerIndex+1] = byte(value >> 7)
		b.writerIndex += 2
		return 2
	}
	if value>>21 == 0 {
		b.grow(3)
		b.data[b.writerIndex] = byte((value & 0x7F) | 0x80)
		b.data[b.writerIndex+1] = byte(value>>7 | 0x80)
		b.data[b.writerIndex+2] = byte(value >> 14)
		b.writerIndex += 3
		return 3
	}
	if value>>28 == 0 {
		b.grow(4)
		b.data[b.writerIndex] = byte((value & 0x7F) | 0x80)
		b.data[b.writerIndex+1] = byte(value>>7 | 0x80)
		b.data[b.writerIndex+2] = byte(value>>14 | 0x80)
		b.data[b.writerIndex+3] = byte(value >> 21)
		b.writerIndex += 4
		return 4
	}
	b.grow(5)
	b.data[b.writerIndex] = byte((value & 0x7F) | 0x80)
	b.data[b.writerIndex+1] = byte(value>>7 | 0x80)
	b.data[b.writerIndex+2] = byte(value>>14 | 0x80)
	b.data[b.writerIndex+3] = byte(value>>21 | 0x80)
	b.data[b.writerIndex+4] = byte(value >> 28)
	b.writerIndex += 5
	return 5
}

// ReadVarInt32 reads the 1-5 byte int part of a varint.
func (b *ByteBuffer) ReadVarInt32() int32 {
	readerIndex := b.readerIndex
	byte_ := int32(b.data[readerIndex])
	readerIndex++
	result := byte_ & 0x7F
	if (byte_ & 0x80) != 0 {
		byte_ = int32(b.data[readerIndex])
		readerIndex++
		result |= (byte_ & 0x7F) << 7
		if (byte_ & 0x80) != 0 {
			byte_ = int32(b.data[readerIndex])
			readerIndex++
			result |= (byte_ & 0x7F) << 14
			if (byte_ & 0x80) != 0 {
				byte_ = int32(b.data[readerIndex])
				readerIndex++
				result |= (byte_ & 0x7F) << 21
				if (byte_ & 0x80) != 0 {
					byte_ = int32(b.data[readerIndex])
					readerIndex++
					result |= (byte_ & 0x7F) << 28
				}
			}
		}
	}
	b.readerIndex = readerIndex
	return result
}

type BufferObject interface {
	TotalBytes() int
	WriteTo(buf *ByteBuffer)
	ToBuffer() *ByteBuffer
}
