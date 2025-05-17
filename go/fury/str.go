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
	"fmt"
	"unicode/utf16"
)

// 编码类型常量
const (
	encodingLatin1 = iota
	encodingUTF16LE
	encodingUTF8
)

// writeString 实现带编码检测的字符串序列化
func writeString(buf *ByteBuffer, value string) error {
	// 检测Latin1编码
	if isLatin1(value) {
		return writeLatin1(buf, value)
	}

	// 检测UTF-16LE编码是否适用
	if utf16Bytes, ok := tryUTF16LE(value); ok {
		return writeUTF16LE(buf, utf16Bytes)
	}

	// 默认使用UTF-8编码
	return writeUTF8(buf, value)
}

// readString 实现带编码解析的字符串反序列化
func readString(buf *ByteBuffer) string {
	header := buf.ReadVarUint64()
	size := header >> 2
	encoding := header & 0b11

	switch encoding {
	case encodingLatin1:
		return readLatin1(buf, int(size))
	case encodingUTF16LE:
		return readUTF16LE(buf, int(size))
	case encodingUTF8:
		return readUTF8(buf, int(size))
	default:
		panic(fmt.Sprintf("invalid string encoding: %d", encoding))
	}
}

// 编码检测辅助函数
func isLatin1(s string) bool {
	for _, r := range s {
		if r > 0xFF {
			return false
		}
	}
	return true
}

func tryUTF16LE(s string) ([]byte, bool) {
	runes := []rune(s)
	utf16Runes := utf16.Encode(runes)

	// 检测代理对
	hasSurrogate := false
	for _, r := range utf16Runes {
		if r >= 0xD800 && r <= 0xDFFF {
			hasSurrogate = true
			break
		}
	}

	if hasSurrogate {
		return nil, false
	}

	// 转换为小端字节序
	buf := make([]byte, 2*len(utf16Runes))
	for i, r := range utf16Runes {
		buf[2*i] = byte(r)
		buf[2*i+1] = byte(r >> 8)
	}
	return buf, true
}

// 具体编码写入方法
func writeLatin1(buf *ByteBuffer, s string) error {
	length := len(s)
	header := (uint64(length) << 2) | encodingLatin1

	buf.WriteVarUint64(header)
	buf.WriteBinary(unsafeGetBytes(s)) // 直接使用底层字节（Latin1字符在Go中与UTF-8兼容）
	return nil
}

func writeUTF16LE(buf *ByteBuffer, data []byte) error {
	length := len(data) / 2
	header := (uint64(length) << 3) | encodingUTF16LE

	buf.WriteVarUint64(header)
	buf.WriteBinary(data)
	return nil
}

func writeUTF8(buf *ByteBuffer, s string) error {
	data := unsafeGetBytes(s)
	header := (uint64(len(data)) << 2) | encodingUTF8

	buf.WriteVarUint64(header)
	buf.WriteBinary(data)
	return nil
}

// 具体编码读取方法
func readLatin1(buf *ByteBuffer, size int) string {
	data := buf.ReadBinary(size)
	return string(data) // Go会自动处理Latin1到UTF-8的转换
}

func readUTF16LE(buf *ByteBuffer, charCount int) string {
	byteCount := charCount * 2
	data := buf.ReadBinary(byteCount)

	u16s := make([]uint16, charCount)
	for i := 0; i < byteCount; i += 2 {
		u16s[i/2] = uint16(data[i]) | uint16(data[i+1])<<8
	}

	return string(utf16.Decode(u16s))
}

func readUTF8(buf *ByteBuffer, size int) string {
	data := buf.ReadBinary(size)
	return string(data)
}
