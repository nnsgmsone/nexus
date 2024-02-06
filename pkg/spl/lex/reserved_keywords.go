package lex

// GetKeywordID returns the lex id of the SQL keyword k or IDENT if k is
// not a keyword.
func GetKeywordID(k string) int32 {
	// The previous implementation generated a map that did a string ->
	// id lookup. Various ideas were benchmarked and the implementation below
	// was the fastest of those, between 3% and 10% faster (at parsing, so the
	// scanning speedup is even more) than the map implementation.
	switch k {
	case "and":
		return AND
	case "as":
		return AS
	case "asc":
		return ASC
	case "bool":
		return BOOL
	case "by":
		return BY
	case "cast":
		return CAST
	case "desc":
		return DESC
	case "eval":
		return EVAL
	case "false":
		return FALSE
	case "float":
		return FLOAT
	case "doube":
		return DOUBLE
	case "int":
		return INT
	case "import":
		return IMPORT
	case "long":
		return LONG
	case "limit":
		return LIMIT
	case "lua":
		return LUA
	case "lua_file":
		return LUA_FILE
	case "not":
		return NOT
	case "or":
		return OR
	case "sort":
		return ORDER
	case "stats":
		return STATS
	case "string":
		return STRING
	case "true":
		return TRUE
	case "type":
		return TYPE
	case "where":
		return WHERE
	case "dedup":
		return DEDUP
	case "extract":
		return EXTRACT
	default:
		return IDENT
	}
}
