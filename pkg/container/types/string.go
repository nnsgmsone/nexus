package types

import (
	"github.com/google/uuid"
	"github.com/nnsgmsone/nexus/pkg/vfs"
	"github.com/nnsgmsone/nexus/pkg/vm/mheap"
)

func (str *String) GetString(area []byte) ([]byte, uuid.UUID, uint64) {
	var uuid uuid.UUID

	if str[1] >= MaxInlineStringLength {
		copy(uuid[:], area[str[0]:str[0]+uint64(len(uuid))])
		return nil, uuid, str[1]
	}
	return area[str[0] : str[0]+str[1]], uuid, str[1]
}

func (str *String) SetString(data []byte, area []byte, fs vfs.FS) ([]byte, error) {
	vlen := len(data)
	voff := len(area)
	if vlen > MaxInlineStringLength {
		uuid := uuid.New()
		if err := fs.WriteFile(uuid.String(), data); err != nil {
			return nil, err
		}
		if voff+len(uuid) >= cap(area) {
			area = mheap.Realloc(area, int64(voff+len(uuid)))[:voff]
		}
		area = append(area[:voff], uuid[:]...)
	} else {
		if voff+vlen >= cap(area) {
			area = mheap.Realloc(area, int64(voff+vlen))
		}
		area = append(area[:voff], data...)
	}
	str[0], str[1] = uint64(voff), uint64(vlen)
	return area, nil
}

func (str *String) SetStringUUID(uuid uuid.UUID, area []byte, size uint64) []byte {
	voff := len(area)
	if voff+len(uuid) >= cap(area) {
		area = mheap.Realloc(area, int64(voff+len(uuid)))[:voff]
	}
	area = append(area[:voff], uuid[:]...)
	str[0], str[1] = uint64(voff), size
	return area
}
