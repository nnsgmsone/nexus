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
