package meta

import (
	"bytes"
	"unicode"
)

type Encoder struct {
	specialChar1 rune
	specialChar2 rune
}

func NewEncoder(specialCh1 rune, specialCh2 rune) *Encoder {
	return &Encoder{
		specialChar1: specialCh1,
		specialChar2: specialCh2,
	}
}

// Encodes the input string to MetaString using adaptive encoding
func (e *Encoder) Encode(input string) MetaString {
	encoding := e.ComputeEncoding(input)
	return e.EncodeWithEncoding(input, encoding)
}

// Encodes the input string to MetaString using specified encoding.
func (e *Encoder) EncodeWithEncoding(input string, encoding Encoding) MetaString {
	if len(input) > 32767 {
		panic("Long meta string than 32767 is not allowed")
	}
	// 根据编码方式执行对应的编码算法
	// length
	length := len(input)
	switch encoding {
	case LOWER_SPECIAL:
		return MetaString{
			inputString:  input,
			encoding:     encoding,
			specialChar1: e.specialChar1,
			specialChar2: e.specialChar2,
			outputBytes:  e.EncodeLowerSpecial(input),
			numBytes:     length,
			numBits:      length * 5,
		}
	case LOWER_UPPER_DIGIT_SPECIAL:
		return MetaString{
			inputString:  input,
			encoding:     encoding,
			specialChar1: e.specialChar1,
			specialChar2: e.specialChar2,
			outputBytes:  e.EncodeUpperDigitSpecial(input),
			numBytes:     length,
			numBits:      length * 6,
		}
	case FIRST_TO_LOWER_SPECIAL:
		return MetaString{
			inputString:  input,
			encoding:     encoding,
			specialChar1: e.specialChar1,
			specialChar2: e.specialChar2,
			outputBytes:  e.EncodeFirstToLowerSpecial(input),
			numBytes:     length,
			numBits:      length * 5,
		}
	case ALL_TO_LOWER_SPECIAL:
		return MetaString{
			inputString:  input,
			encoding:     encoding,
			specialChar1: e.specialChar1,
			specialChar2: e.specialChar2,
			outputBytes:  e.EncodeAllToLowerSpecial(input),
			numBytes:     length,
			numBits:      (countUppers(input) + length) * 5,
		}
	default:
		// UTF-8 Encoding, Stay the same
		outputBytes := []byte(input)
		return MetaString{
			inputString:  input,
			encoding:     encoding,
			specialChar1: e.specialChar1,
			specialChar2: e.specialChar2,
			outputBytes:  outputBytes,
			numBytes:     len(outputBytes),
			numBits:      len(outputBytes) * 8,
		}
	}
}

func (e *Encoder) EncodeLowerSpecial(input string) []byte {
	return e.EncodeGeneric([]byte(input), 5)
}

func (e *Encoder) EncodeUpperDigitSpecial(input string) []byte {
	return e.EncodeGeneric([]byte(input), 6)
}

func (e *Encoder) EncodeFirstToLowerSpecial(input string) []byte {
	// all chars in string are ASCII, so we can modify input[0] directly
	chars := []byte(input)
	bytes.ToLower(chars[0:1])
	return e.EncodeGeneric(chars, 5)
}

func (e *Encoder) EncodeAllToLowerSpecial(input string) []byte {
	chars := make([]byte, 0)
	for _, c := range input {
		if unicode.IsUpper(c) {
			chars = append(chars, '|')
			chars = append(chars, byte(c))
		} else {
			chars = append(chars, byte(c))
		}
	}
	return e.EncodeGeneric(chars, 5)
}

func (e *Encoder) EncodeGeneric(chars []byte, bitsPerChar int) []byte {
	totBits := len(chars) * bitsPerChar
	totBytes := (totBits + 7) / 8
	result := make([]byte, totBytes)
	currentBit := 0
	for _, c := range chars {
		var value byte
		if bitsPerChar == 5 {
			value = e.charToValueLowerSpecial(c)
		} else if bitsPerChar == 6 {
			value = e.charToValueLowerUpperDigitSpecial(c)
		}
		// 根据 currentBit 算出应该填入到 result 的哪个位置
		// abc 编码后为 [00000] [000,01] [00010] [0, 对应到三个字节, 为 0, 68, 0 (68 = 64 + 4)
		// 按照顺序, 先放最高位, 再放最低位
		for i := bitsPerChar - 1; i >= 0; i-- {
			if value & (1 << i) {
				bytePos := currentBit / 8
				bitPos := currentBit % 8
				result[bytePos] |= 1 << (7 - bitPos)
			}
			currentBit++
		}
	}
	return result
}

func (e *Encoder) ComputeEncoding(input string) Encoding {
	statistics := e.computeStringStatistics(input)
	if statistics.canLowerSpecialEncoded {
		return LOWER_SPECIAL
	}
	if statistics.canLowerUpperDigitSpecialEncoded {
		// 字符串中均为字母, 数字, 和两个特殊符号
		if statistics.digitCount != 0 {
			return LOWER_UPPER_DIGIT_SPECIAL
		}
		upperCount := statistics.upperCount
		chars := []rune(input)
		if upperCount == 1 && unicode.IsUpper(chars[0]) {
			return FIRST_TO_LOWER_SPECIAL
		}
		if (len(chars)+upperCount)*5 < len(chars)*6 {
			return ALL_TO_LOWER_SPECIAL
		}
		return LOWER_UPPER_DIGIT_SPECIAL
	}
	return UTF_8
}

type stringStatistics struct {
	digitCount                       int
	upperCount                       int
	canLowerSpecialEncoded           bool
	canLowerUpperDigitSpecialEncoded bool
}

func (e *Encoder) computeStringStatistics(input string) *stringStatistics {
	digitCount, upperCount := 0, 0
	canLowerSpecialEncoded := true
	canLowerUpperDigitSpecialEncoded := true
	for _, c := range input {
		if canLowerUpperDigitSpecialEncoded {
			if !(c >= 'a' && c <= 'z' || c >= 'A' && c <= 'Z' || c >= '0' && c <= '9' || c == e.specialChar1 || c == e.specialChar2) {
				canLowerUpperDigitSpecialEncoded = false
			}
		}

		if canLowerSpecialEncoded {
			if !(c >= 'a' && c <= 'z' || c == '.' || c == '_' || c == '$' || c == '|') {
				canLowerSpecialEncoded = false
			}
		}

		if unicode.IsDigit(c) {
			digitCount++
		}

		if unicode.IsUpper(c) {
			upperCount++
		}
	}
	return &stringStatistics{
		digitCount:                       digitCount,
		upperCount:                       upperCount,
		canLowerSpecialEncoded:           canLowerSpecialEncoded,
		canLowerUpperDigitSpecialEncoded: canLowerUpperDigitSpecialEncoded,
	}
}

func countUppers(str string) int {
	cnt := 0
	for _, c := range str {
		if unicode.IsUpper(c) {
			cnt++
		}
	}
	return cnt
}

func (e *Encoder) charToValueLowerSpecial(c byte) byte {
	if c >= 'a' && c <= 'z' {
		return c - 'a'
	} else if c == '.' {
		return 26
	} else if c == '_' {
		return 27
	} else if c == '$' {
		return 28
	} else if c == '|' {
		return 29
	} else {
		panic("Unsupported character for LOWER_SPECIAL encoding: {c}")
	}
}

func (e *Encoder) charToValueLowerUpperDigitSpecial(c byte) byte {
	if c >= 'a' && c <= 'z' {
		return c - 'a'
	} else if c >= 'A' && c <= 'Z' {
		return 26 + (c - 'A')
	} else if c >= '0' && c <= '9' {
		return 52 + (c - '0')
	} else if rune(c) == e.specialChar1 {
		return 62
	} else if rune(c) == e.specialChar2 {
		return 63
	} else {
		panic("Unsupported character for LOWER_UPPER_DIGIT_SPECIAL encoding: {c}")
	}
}
