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
