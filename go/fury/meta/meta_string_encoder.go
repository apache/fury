package meta

import "unicode"

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
	return nil
}

func (e *Encoder) EncodeUpperDigitSpecial(input string) []byte {
	return nil
}

func (e *Encoder) EncodeFirstToLowerSpecial(input string) []byte {
	return nil
}

func (e *Encoder) EncodeAllToLowerSpecial(input string) []byte {
	return nil
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
