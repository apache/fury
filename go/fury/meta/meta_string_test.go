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
	str := decoder.Decode(data.GetEncodedBytes(), LOWER_SPECIAL, 7*5)
	require.Equal(t, len(data.GetEncodedBytes()), 5)
	require.Equal(t, str, "abc_def")

	// "org.apache.fury.benchmark.data"
	data = encoder.Encode("org.apache.fury.benchmark.data")
	str = decoder.Decode(data.GetEncodedBytes(), LOWER_SPECIAL, data.GetNumBits())
	require.Equal(t, "org.apache.fury.benchmark.data", str)

	// "MediaContent"
	data = encoder.Encode("MediaContent")
	str = decoder.Decode(data.GetEncodedBytes(), data.GetEncoding(), data.GetNumBits())
	require.Equal(t, "MediaContent", str)
	require.Equal(t, data.GetNumBits(), 70)

	// "HelloWorld__123.2024"
	data = encoder.Encode("HelloWorld__123.2024")
	str = decoder.Decode(data.GetEncodedBytes(), data.GetEncoding(), data.GetNumBits())
	require.Equal(t, "HelloWorld__123.2024", str)
	require.Equal(t, data.GetNumBits(), data.GetNumChars()*6)

}
