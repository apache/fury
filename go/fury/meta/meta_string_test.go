package meta

import (
	"github.com/stretchr/testify/require"
	"testing"
)

func TestEncodeMetaStringLowerSpecial(t *testing.T) {
	// "abc_def"
	encoder := NewEncoder('.', '_')
	data := encoder.Encode("abc_def")
	decoder := NewDecoder('.', '_')
	str := decoder.Decode(data.GetOutputBytes(), LOWER_SPECIAL, 7*5)
	require.Equal(t, len(data.GetOutputBytes()), 5)
	require.Equal(t, str, "abc_def")

	// "org.apache.fury.benchmark.data"
	data = encoder.Encode("org.apache.fury.benchmark.data")
	str = decoder.Decode(data.GetOutputBytes(), LOWER_SPECIAL, data.GetNumBits())
	require.Equal(t, "org.apache.fury.benchmark.data", str)

	// "MediaContent"
	data = encoder.Encode("MediaContent")
	str = decoder.Decode(data.GetOutputBytes(), data.GetEncoding(), data.GetNumBits())
	require.Equal(t, "MediaContent", str)
	require.Equal(t, data.GetNumBits(), 70)

	// "HelloWorld__123.2024"
	data = encoder.Encode("HelloWorld__123.2024")
	str = decoder.Decode(data.GetOutputBytes(), data.GetEncoding(), data.GetNumBits())
	require.Equal(t, "HelloWorld__123.2024", str)
	require.Equal(t, data.GetNumBits(), data.GetNumChars()*6)

}
