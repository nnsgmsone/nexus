package main

import "github.com/abiosoft/readline"

var completer = readline.NewPrefixCompleter(
	readline.PcItem("|",
		readline.PcItem("and"),
		readline.PcItem("as"),
		readline.PcItem("asc"),
		readline.PcItem("bool"),
		readline.PcItem("by"),
		readline.PcItem("cast"),
		readline.PcItem("desc"),
		readline.PcItem("end"),
		readline.PcItem("eval"),
		readline.PcItem("false"),
		readline.PcItem("fields"),
		readline.PcItem("double"),
		readline.PcItem("from"),
		readline.PcItem("long"),
		readline.PcItem("limit"),
		readline.PcItem("load"),
		readline.PcItem("not"),
		readline.PcItem("or"),
		readline.PcItem("sort"),
		readline.PcItem("start"),
		readline.PcItem("stats"),
		readline.PcItem("string"),
		readline.PcItem("true"),
		readline.PcItem("type"),
		readline.PcItem("where"),
		readline.PcItem("extract"),
		readline.PcItem("regexp"),
		readline.PcItem("chunk"),
		readline.PcItem("like"),
		readline.PcItem("cast"),
		readline.PcItem("line_extract"),
		readline.PcItem("regexp_extract"),
	),
	readline.PcItem("help"),
	readline.PcItem("quit"),
	readline.PcItem("exit"),
	readline.PcItem("clear"),
)
