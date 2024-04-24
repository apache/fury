package meta

import (
	"strings"
)

type Decoder struct {
	specialChar1 byte
	specialChar2 byte
}

func NewDecoder(specialCh1 byte, specialCh2 byte) *Decoder {
	return &Decoder{
		specialChar1: specialCh1,
		specialChar2: specialCh2,
	}
}

// 接受一个编码后的 byte 数组, 以及编码方式, 解码
// 还需要接受 numBits, 因为不知道 byte 数组里究竟包含了多少个字符, numBits 是实际有效的比特位
func (d *Decoder) Decode(data []byte, encoding Encoding, numBits int) string {
	switch encoding {
	case LOWER_SPECIAL:
		return d.decodeGeneric(data, encoding, numBits)
	case LOWER_UPPER_DIGIT_SPECIAL:
		return d.decodeGeneric(data, encoding, numBits)
	case FIRST_TO_LOWER_SPECIAL:
		return strings.ToTitle(d.decodeGeneric(data, LOWER_SPECIAL, numBits))
	case ALL_TO_LOWER_SPECIAL:
		return d.decodeRepAllToLowerSpecial(data, LOWER_SPECIAL, numBits)
	case UTF_8:
		return string(data)
	default:
		panic("Unexpected encoding flag: {encoding}")
	}
}

func (d *Decoder) decodeGeneric(data []byte, algorithm Encoding, numBits int) string {
	bitsPerChar := 5
	if algorithm == LOWER_UPPER_DIGIT_SPECIAL {
		bitsPerChar = 6
	}
	// 从 data 中每次取出 5 个 bit, 转换成字符, 然后存到 chars 中
	// abc 编码后为 [00000] [000,01] [00010] [0, 对应到三个字节, 为 0, 68, 0 (68 = 64 + 4)
	// 按照顺序, 先取最高位, 再取最低位
	chars := make([]byte, 0)
	bitPos, bitCount := 7, 0
	for bitCount+bitsPerChar <= numBits {
		var val byte = 0
		for i := 0; i < bitsPerChar; i++ {
			val <<= 1
			if data[bitCount/8]&(1<<bitPos) > 0 {
				val += 1
			}
			bitPos = (bitPos - 1 + 8) % 8
			bitCount++
		}
		chars = append(chars, d.decodeChar(val, algorithm))
	}
	return string(chars)
}

func (d *Decoder) decodeRepAllToLowerSpecial(data []byte, algorithm Encoding, numBits int) string {
	// 先按照小写字母解码, 再做转换
	str := d.decodeGeneric(data, algorithm, numBits)
	chars := make([]byte, 0)
	for i := 0; i < len(str); i++ {
		if str[i] == '|' {
			chars = append(chars, str[i+1]-'a'+'A')
			i++
		} else {
			chars = append(chars, str[i])
		}
	}
	return string(chars)
}

/** Decoding char for two encoding algorithms */
func (d *Decoder) decodeChar(val byte, encoding Encoding) byte {
	switch encoding {
	case LOWER_SPECIAL:
		return d.decodeLowerSpecialChar(val)
	case LOWER_UPPER_DIGIT_SPECIAL:
		return d.decodeLowerUpperDigitSpecialChar(val)
	}
	panic("Illegal encoding flag")
}

/** Decoding char for LOWER_SPECIAL Encoding Algorithm */
func (d *Decoder) decodeLowerSpecialChar(charValue byte) byte {
	if charValue <= 25 {
		return 'a' + charValue
	} else if charValue == 26 {
		return '.'
	} else if charValue == 27 {
		return '_'
	} else if charValue == 28 {
		return '$'
	} else if charValue == 29 {
		return '|'
	} else {
		panic("Invalid character value for LOWER_SPECIAL: {charValue}")
	}
}

/** Decoding char for LOWER_UPPER_DIGIT_SPECIAL Encoding Algorithm. */
func (d *Decoder) decodeLowerUpperDigitSpecialChar(charValue byte) byte {
	if charValue <= 25 {
		return 'a' + charValue
	} else if charValue >= 26 && charValue <= 51 {
		return 'A' + (charValue - 26)
	} else if charValue >= 52 && charValue <= 61 {
		return '0' + (charValue - 52)
	} else if charValue == 62 {
		return d.specialChar1
	} else if charValue == 63 {
		return d.specialChar2
	} else {
		panic("Invalid character value for LOWER_UPPER_DIGIT_SPECIAL: {charValue}")
	}
}
