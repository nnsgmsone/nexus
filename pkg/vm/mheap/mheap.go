package mheap

func Alloc(size int64) []byte {
	return make([]byte, size)
}

func Realloc(data []byte, size int64) []byte {
	rdata := make([]byte, realloc(data, size))
	copy(rdata, data)
	return rdata
}

func realloc(data []byte, size int64) int64 {
	if data == nil {
		return size
	}
	n := int64(cap(data))
	if size <= n {
		return n
	}
	newcap := n
	doublecap := n + n
	if size > doublecap {
		newcap = size
	} else {
		if len(data) < 1024 {
			newcap = doublecap
		} else {
			for 0 < newcap && newcap < size {
				newcap += newcap / 4
			}
			if newcap <= 0 {
				newcap = size
			}
		}
	}
	return newcap
}
