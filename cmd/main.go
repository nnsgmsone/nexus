package main

import (
	"fmt"
	"os"
	"strings"
	"sync"
	"time"

	"github.com/abiosoft/readline"
	"github.com/nnsgmsone/nexus/pkg/container/batch"
	"github.com/nnsgmsone/nexus/pkg/spl/compile"
	"github.com/nnsgmsone/nexus/pkg/vfs"
	"github.com/nnsgmsone/nexus/pkg/vm/engine/noah"
	"github.com/nnsgmsone/nexus/pkg/vm/process"
	"github.com/olekukonko/tablewriter"
)

const (
	DefaultDir = "nexus-data"
)

type runner struct {
	sync.Mutex
	rows int
	line []string
	w    *tablewriter.Table
}

func main() {
	fs := vfs.NewFS()
	if _, err := fs.Stat(DefaultDir); err != nil {
		if os.IsNotExist(err) {
			if err = fs.Mkdir(DefaultDir); err != nil {
				panic(err)
			}
		} else {
			panic(err)
		}
	}
	if err := fs.ChDir(DefaultDir); err != nil {
		panic(err)
	}
	e, err := noah.New(fs)
	if err != nil {
		panic(err)
	}
	rl, err := readline.NewEx(&readline.Config{
		Prompt:          "nexus> ",
		HistoryFile:     "/tmp/nexus.tmp",
		AutoComplete:    completer,
		InterruptPrompt: "^C",
	})
	if err != nil {
		panic(err)
	}
	defer rl.Close()
	fmt.Println("Welcome to nexus")
	for {
		line, err := rl.Readline()
		if err != nil {
			fmt.Println(err.Error())
			return
		}
		if strings.ToLower(line) == "exit" || strings.ToLower(line) == "quit" {
			fmt.Println("Goodbye.")
			return
		}
		if strings.ToLower(line) == "clear" {
			fmt.Print("\033[H\033[2J")
			continue
		}
		if strings.ToLower(line) == "help" {
			fmt.Println("quit")
			fmt.Println("exit")
			fmt.Println("help")
			fmt.Println("clear")
			fmt.Println("| spl")
			continue
		}
		rl.SaveHistory(line)
		t := time.Now()
		r := &runner{
			w: tablewriter.NewWriter(os.Stdout),
		}
		fmt.Printf("run spl '%s'\n", line)
		c, err := compile.New(line, e, process.New(fs), showResult(r))
		if err != nil {
			fmt.Println(err.Error())
			continue
		}
		r.setHeader(c.Columns())
		if err := c.Compile(); err != nil {
			fmt.Println(err.Error())
			continue
		}
		if err := c.Run(); err != nil {
			fmt.Printf("run spl '%s' failed: %v\n", line, err)
			continue
		}
		fmt.Printf("run spl '%s' success rows: %v: process %v\n", line, r.rows, time.Now().Sub(t))
	}
}

func showResult(r *runner) func(*batch.Batch) error {
	return func(bat *batch.Batch) error {
		r.Lock()
		defer r.Unlock()
		r.rows += bat.Rows()
		for i := 0; i < bat.Rows(); i++ {
			for j := 0; j < bat.VectorCount(); j++ {
				r.line[j] = bat.GetVector(j).GetValueString(i)
			}
			r.w.Append(r.line)
		}
		r.w.Render()
		return nil
	}
}

func (r *runner) setHeader(cols []string) {
	r.w.SetHeader(cols)
	r.w.SetRowLine(true)
	r.w.SetCenterSeparator("*")
	r.w.SetColumnSeparator("|")
	r.w.SetRowSeparator("-")
	r.w.SetAlignment(tablewriter.ALIGN_LEFT)
	r.line = make([]string, len(cols))
}
