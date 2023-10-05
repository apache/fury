package fury

import (
	"github.com/stretchr/testify/require"
	"testing"
	"time"
)

func TestSnake(t *testing.T) {
	require.Equal(t, "a_bcd_efg_hij", SnakeCase("aBcdEfgHij"))
	require.Equal(t, "a_bcd_efg_hij", SnakeCase("ABcdEfgHij"))
	require.Equal(t, "a_b_c_d_efg_hij", SnakeCase("ABCDEfgHij"))
	require.Equal(t, SnakeCase("ToSnake"), "to_snake")
	require.Equal(t, SnakeCase("toSnake"), "to_snake")
	require.Equal(t, SnakeCase("to_snake"), "to_snake")
	require.Equal(t, SnakeCase("AbcAbcAbc"), "abc_abc_abc")
	require.Equal(t, SnakeCase("ABC"), "a_b_c")
}

func TestTime(t *testing.T) {
	t1 := time.Now()
	ts := GetUnixMicro(t1)
	t2 := CreateTimeFromUnixMicro(ts)
	require.Equal(t, t1.Second(), t2.Second())
	// Micro doesn't preserve Nanosecond precision.
	require.Equal(t, t1.Nanosecond()/1000, t2.Nanosecond()/1000)
	require.WithinDuration(t, t1, t2, 1000)
}
