package parser

import (
	"fmt"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestParser(t *testing.T) {
	stmt, err := Parse("| extract lua =  `csv.lua` a=0, b =1 | limit 1 | eval d = a+b | sort 10 by uid | stats count(host) as cnt by ip")
	require.NoError(t, err)
	fmt.Printf("%s\n", stmt)
	stmt, err = Parse("| extract lua_file = `csv.lua`  a=0, b=2 | limit 1 | eval d = a+b | sort by uid | dedup uid")
	require.NoError(t, err)
	fmt.Printf("%s\n", stmt)
	stmt, err = Parse("| extract lua = `json.lua` a=2, b = 1 | stats count() by uid | stats sum(b+1) as c")
	require.NoError(t, err)
	fmt.Printf("%s\n", stmt)
	stmt, err = Parse("| import \"1.csv\", \"2.csv\"")
	require.NoError(t, err)
	fmt.Printf("%s\n", stmt)
}
