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
