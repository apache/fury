package fury

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"github.com/apache/fury/go/fury/meta"
	"github.com/spaolacci/murmur3"
)

const (
	SmallStringThreshold         = 8
	DefaultDynamicWriteMetaStrID = -1
)

type Encoding int8

type MetaStringBytes struct {
	Data                 []byte
	Length               int16
	Encoding             meta.Encoding
	Hashcode             int64
	DynamicWriteStringID int16
}

func NewMetaStringBytes(data []byte, hashcode int64) *MetaStringBytes {
	return &MetaStringBytes{
		Data:                 data,
		Length:               int16(len(data)),
		Hashcode:             hashcode,
		Encoding:             meta.Encoding(hashcode & 0xFF),
		DynamicWriteStringID: DefaultDynamicWriteMetaStrID,
	}
}

func (a *MetaStringBytes) Equals(b *MetaStringBytes) bool {
	return a.Hashcode == b.Hashcode
}

func (a *MetaStringBytes) Hash() int64 {
	return a.Hashcode
}

type pair [2]int64

type MetaStringResolver struct {
	dynamicWriteStringID     int16
	dynamicWrittenEnumString []*MetaStringBytes
	dynamicIDToEnumString    []*MetaStringBytes
	hashToMetaStrBytes       map[int64]*MetaStringBytes
	smallHashToMetaStrBytes  map[pair]*MetaStringBytes
	enumStrSet               map[*MetaStringBytes]struct{}
	metaStrToMetaStrBytes    map[interface{}]*MetaStringBytes
}

func NewMetaStringResolver() *MetaStringResolver {
	return &MetaStringResolver{
		hashToMetaStrBytes:      make(map[int64]*MetaStringBytes),
		smallHashToMetaStrBytes: make(map[pair]*MetaStringBytes),
		enumStrSet:              make(map[*MetaStringBytes]struct{}),
		metaStrToMetaStrBytes:   make(map[interface{}]*MetaStringBytes),
	}
}

func (r *MetaStringResolver) WriteMetaStringBytes(buf *ByteBuffer, m *MetaStringBytes) error {
	if m.DynamicWriteStringID == DefaultDynamicWriteMetaStrID {
		m.DynamicWriteStringID = r.dynamicWriteStringID
		r.dynamicWriteStringID++
		r.dynamicWrittenEnumString = append(r.dynamicWrittenEnumString, m)

		header := uint32(m.Length) << 1
		if err := writeVarUint32(buf, header); err != nil {
			return err
		}

		if m.Length <= SmallStringThreshold {
			buf.WriteByte(byte(m.Encoding))
		} else {
			err := binary.Write(buf, binary.LittleEndian, m.Hashcode)
			if err != nil {
				return err
			}
		}
		buf.Write(m.Data)
	} else {
		header := uint32((m.DynamicWriteStringID+1)<<1) | 1
		err := writeVarUint32(buf, header)
		if err != nil {
			return err
		}
	}
	return nil
}

func (r *MetaStringResolver) ReadMetaStringBytes(buf *ByteBuffer) (*MetaStringBytes, error) {
	header, err := readVarUint32(buf)
	if err != nil {
		return nil, err
	}

	length := int16(header >> 1)
	if header&1 != 0 {
		index := int(length) - 1
		if index >= len(r.dynamicIDToEnumString) {
			return nil, fmt.Errorf("invalid dynamic index: %d", index)
		}
		return r.dynamicIDToEnumString[index], nil
	}

	var (
		hashcode int64
		key      pair
		data     []byte
		encoding Encoding
	)

	if length <= SmallStringThreshold {
		encByte, _ := buf.ReadByte()
		encoding = Encoding(encByte)

		data = make([]byte, length)
		_, err := buf.Read(data)
		if err != nil {
			return nil, err
		}

		if length <= 8 {
			key[0] = bytesToInt64(data)
		} else {
			err := binary.Read(bytes.NewReader(data[:8]), binary.LittleEndian, &key[0])
			if err != nil {
				return nil, err
			}
			key[1] = bytesToInt64(data[8:])
		}
		hashcode = ((key[0]*31 + key[1]) >> 8 << 8) | int64(encoding)
	} else {
		err := binary.Read(buf, binary.LittleEndian, &hashcode)
		if err != nil {
			return nil, err
		}
		encoding = Encoding(hashcode & 0xFF)
		data = make([]byte, length)
		_, err = buf.Read(data)
		if err != nil {
			return nil, err
		}
	}

	if length <= SmallStringThreshold {
		if m, ok := r.smallHashToMetaStrBytes[key]; ok {
			r.dynamicIDToEnumString = append(r.dynamicIDToEnumString, m)
			return m, nil
		}
	} else {
		if m, ok := r.hashToMetaStrBytes[hashcode]; ok {
			r.dynamicIDToEnumString = append(r.dynamicIDToEnumString, m)
			return m, nil
		}
	}

	m := NewMetaStringBytes(data, hashcode)
	if length <= SmallStringThreshold {
		r.smallHashToMetaStrBytes[key] = m
	} else {
		r.hashToMetaStrBytes[hashcode] = m
	}
	r.enumStrSet[m] = struct{}{}
	r.dynamicIDToEnumString = append(r.dynamicIDToEnumString, m)

	return m, nil
}

func (r *MetaStringResolver) GetMetaStrBytes(metastr interface{}) *MetaStringBytes {
	if m, exists := r.metaStrToMetaStrBytes[metastr]; exists {
		return m
	}

	var hashcode int64
	data := metastr.(interface{ GetEncodedData() []byte }).GetEncodedData()
	length := len(data)

	if length <= SmallStringThreshold {
		var v1, v2 int64
		if length <= 8 {
			v1 = bytesToInt64(data)
		} else {
			binary.Read(bytes.NewReader(data[:8]), binary.LittleEndian, &v1)
			v2 = bytesToInt64(data[8:])
		}
		hashcode = ((v1*31 + v2) >> 8 << 8) | int64(uint8(metastr.(interface{ GetEncoding() Encoding }).GetEncoding()))
	} else {
		hash := murmur3.New128()
		hash.Write(data)
		h1, h2 := hash.Sum128()
		hashcode = (int64(h1)<<32 | int64(h2)) >> 8 << 8
		hashcode |= int64(uint8(metastr.(interface{ GetEncoding() Encoding }).GetEncoding()))
	}

	m := NewMetaStringBytes(data, hashcode)
	r.metaStrToMetaStrBytes[metastr] = m
	return m
}

func (r *MetaStringResolver) ResetRead() {
	r.dynamicIDToEnumString = nil
}

func (r *MetaStringResolver) ResetWrite() {
	r.dynamicWriteStringID = 0
	for _, m := range r.dynamicWrittenEnumString {
		m.DynamicWriteStringID = DefaultDynamicWriteMetaStrID
	}
	r.dynamicWrittenEnumString = nil
}

// Helper functions
func writeVarUint32(buf *ByteBuffer, v uint32) error {
	for v >= 0x80 {
		buf.WriteByte(byte(v) | 0x80)
		v >>= 7
	}
	buf.WriteByte(byte(v))
	return nil
}

func readVarUint32(buf *ByteBuffer) (uint32, error) {
	var x uint32
	var s uint
	for {
		b, err := buf.ReadByte()
		if err != nil {
			return 0, err
		}
		x |= uint32(b&0x7F) << s
		if b < 0x80 {
			break
		}
		s += 7
	}
	return x, nil
}

func bytesToInt64(b []byte) int64 {
	var v int64
	for i := range b {
		v |= int64(b[i]) << (8 * i)
	}
	return v
}
