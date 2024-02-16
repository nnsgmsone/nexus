# Nexus

Nexus is an interactive tool designed for versatile data analysis, leveraging the SPL (Search Processing Language) for expressing data manipulation tasks. Released under the GNU General Public License (GPL), Nexus offers the freedom to utilize, modify, and distribute its code.

Utilizing SPL, Nexus excels at parsing unformatted data, employing a syntax reminiscent of Unix pipelines with the pipe symbol (|). Each command within the pipeline encapsulates a specific analysis task.


The SPL syntax follows this pattern:

```sql
| <spl commands> = | <spl command> | <spl command> | ...
```

## Key Features

* Fast import
* Parsing data in arbitrary formats
* Extending data parsing capabilities with Lua plugins
* Data analysis through SPL
* Interactive user interface

## Introduction to Nexus

### Nexus Architecture

Nexus takes all imported data and streams it in the form of byte streams to the extract command. The extract command parses the byte stream and transforms it into a table format, resembling a database format, based on specific processing logic. The resulting table is then passed on to the next command. Ultimately, after all commands have been executed, the results are displayed on the screen.

In addition to the extract command, Nexus includes other commands such as dedup, where, eval, sort, limit, stats, etc. These commands are designed to manipulate the data. It is important to note that, excluding the extract command, the input and output for these commands are both in table format.

### Compiling Nexus
```bash
make
```
### Launching Nexus
```bash
# This command initiates an interactive interface to execute commands for data analysis
./nexus
```
### Interactive Commands in Nexus
Nexus responds to several interactive commands:
* quit/exit: Exit Nexus
* clear: Clear the screen
* Arrow keys: Navigate command history
* Supported SPL statements

### File Path
File Path in nexus all use absolute paths

### Importing Data
Nexus leverages the import statement to import data, following this syntax:

```sql
IMPORT name [, name]...
```
Here are two examples:

```sql
| import "file1"; -- Import file1
| import "file1", "file2"; -- Import file1 and file2
```

### Clearing Data

Nexus uses the clean statement to clear data, employing the following syntax:

```sql
CLEAN
```
Here is an example:

```sql
| clean
```
### Parsing Data
Data parsing in Nexus utilizes the extract statement, serving as the initial command in a query. The syntax for extract is as follows:

```sql
EXTRACT [LUA = STRING | LUA_FILE = STRING] eval_list
```
Consider this example:

```sql
| extract LUA_FILE="1.lua" a = 0, b = 1, c = 2 | eval a = cast(a as float) + cast(b as float) | sort 10 by a;
```
The extract command incorporates Lua to implement the basic logic of data parsing. Details on writing Lua scripts for various functionalities will be explained in the Lua script section.

### Deduplication
Data deduplication in Nexus is achieved through the dedup statement, utilizing the following syntax:

```sql
dedup name [, name]
```
Consider these examples:

```sql
... | dedup a
... | dedup a, b
```

### Filtering
Nexus filters data using the where statement, adhering to the syntax:

```sql
where expr
```
Consider these simple examples:

```sql
... | where a = 1
... | where like(ip, "198.*")
```

### Projection
Nexus projects data through the eval statement, following the syntax:

```sql
eval name = expr [, name = expr]
```
Consider these simple examples:

```sql
... | eval a = b + c
... | eval a = cast(b as float)
```

### Sorting
Data sorting in Nexus is achieved through the sort statement, with the syntax:

```sql
sort [int] sort-field-list
sort-field-list = name [desc|asc] [, name [desc|asc]]
```
Consider these simple examples:

```sql
... | sort by uid
... | sort 10 by uid, date
```

### Limiting
Limiting data in Nexus is accomplished using the limit statement, following this syntax:

```sql
limit int
```
Consider this example:

```sql
... | limit 10
```

### Grouping and Aggregation
Nexus employs the stats statement for grouping and aggregating data, with the syntax:

```sql
stats expr [as name] [, expr [as name]] [by name [, name]]
```
Currently supported aggregation functions for stats include:

* count
* sum
* max
* min
* avg

Consider these examples:

```sql
... | stats count()
... | stats count() as cnt by ip
... | stats sum(a), avg(a) by b, c
```

Functions and Operators
Nexus currently supports various functions and operators:

* isnull(x): Checks if x is nil.
* isnotnull(x): Checks if x is not nil.
* cast(x as t): Converts x to type t.
* replace(x, old, new): String replacement.
* regexp_match(x, reg): Checks if x matches the regular expression reg.
* regexp_extract(x, reg, idx): Extracts data that satisfies the conditions.
* Arithmetic operators: +, -, *, /, <, <=, >=, >, =, <>, %
* Logical operators: and, or, not

###  Data Types
Nexus supports the following data types:

* bool
* long
* double
* string
* NULL - Represents non-existence

### Constants
The supported constants in Nexus are as follows:
* true, false
* 1
* 1.2
* "x", \`x\`

Nexus supports two types of strings. The first type is similar to "x", where the content within double quotation marks is considered the string content. It is important to note that this representation automatically handles escape characters, for example, "\n" will be treated as a newline character. The second type is \`x\`-style strings, where the content within backticks is considered the string content and no escape processing is applied.

### Lua Scripting

Nexus utilizes a Lua script to convert byte stream data into table data. The Lua script employs io.read() to continuously read data, parsing it into table data and sending data chunks to Nexus.

The Lua script outputs a table with n rows * n columns, representing a two-dimensional array using Lua's table data structure. The specific format of the two-dimensional array is exemplified as follows:

```lua
tbl = { {"x", "y", "z"}, {"a", "b", "c"}}
```

The processing methodology involves using a column-by-column, one-dimensional array. Each output is transmitted to Nexus through writeResult. It is important to note that the rows in each column need to be aligned, and the number of rows must not exceed 8192, necessitating the script to output data in manageable chun


Below are several examples of Lua scripts. The first script separates the input byte stream into lines, with each line containing only one column of data:

```lua
local row = 0
local tbl = {{}}
tbl[1] = tbl[1] or {}
for line in io.stdin:lines() do
    local rows = #tbl[1] or 0
    if rows >= 8192 then
        writeResult(tbl)
        row = 0
        tbl = {{}}
        tbl[1] = tbl[1] or {}
    end
    tbl[1][row+1] = line
    row = row + 1
end
local rows = #tbl[1] or 0
if rows > 0 then
    writeResult(tbl)
end
writeResult(nil)
```	

This is a simple example, and all Lua scripts are similar. They read data through io.stdin, build a two-dimensional array, and call writeResult to return results in batches, using writeResult(nil) at the end.

The next example is a more complex one designed to parse a simple CSV format. The Lua script is as follows:

```lua
-- csv.lua - parse and format csv
-- Usage: cat csv.txt | lua csv.lua
-- Input: csv.txt
-- Output: {field1, field2, field3..},{field1, field2, field3...}...
-- Author: nnsgmsone
function parseCSVLine(line)
    local fields = {}
    for v in string.gmatch(line, '([^,]+)') do
        table.insert(fields, v)
    end
    return fields
end

local row = 0
local tbl = {{}}
for line in io.stdin:lines() do
    local record = parseCSVLine(line)
    local rows = #tbl[1] or 0
    if rows >= 8192 then
        writeResult(tbl)
        row = 0
        tbl = {{}}
    end
    for i, v in ipairs(record) do
        tbl[i] = tbl[i] or {}
        tbl[i][row+1] = v
    end
    row = row + 1
end
local rows = #tbl[1] or 0
if rows > 0 then
    writeResult(tbl)
end
writeResult(nil)
```

The difference from the first Lua script is that this script outputs multiple columns. It parses CSV, divides the CSV data into units of 8192 rows, and then returns the data in table format (column storage).

The last example is a more practical Lua script that parses dump of goroutines, converting each goroutine into three columns (creator information, call stack information, goroutine survival time). The Lua script is as follows:

```lua
-- goroutine.lua - parse and format goroutine dump
-- Usage: cat goroutine.txt | lua goroutine.lua
-- Input: goroutine.txt
-- Output: {created, stack, time},{created, stack, time}...
-- Author: nnsgmsone
local row = 0
local tbl = {{}}
local time = "0"
local stack = ""
local created = ""
tbl[1] = tbl[1] or {}
tbl[2] = tbl[2] or {}
tbl[3] = tbl[3] or {}
for line in io.stdin:lines() do
    local rows = #tbl[1] or 0
    if rows >= 8192 then
        writeResult(tbl)
        row = 0
        tbl = {{}}
        tbl[1] = tbl[1] or {}
        tbl[2] = tbl[2] or {}
        tbl[3] = tbl[3] or {}
    end
    if string.len(stack) == 0 then
        local find = string.match(line, "goroutine")
        if find  then
            stack = line
            local t = string.match(line, "(%d+)%s*minutes")
            if t then 
                time = t
            end
        end
    elseif string.len(created) > 0 then
        local find = string.match(line, "%S+")
        if find then 
            tbl[1][row+1] = find
        else
            tbl[1][row+1] = ""
        end
        tbl[2][row+1] = stack .. "\n" .. line
        tbl[3][row+1] = time
        created = ""
        stack = ""
        time = "0"
        row = row + 1
    else
        local find = string.match(line, "created by")
        if find then
            created = find
        else
            stack = stack .. "\n" .. line
        end
    end
end
writeResult(tbl)
writeResult(nil)
```

These Lua scripts provide examples of reading and processing data in different formats.

## License
This project is licensed under the GPL [License](/LICENSE).
