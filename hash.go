package cvdb

import "encoding/binary"

type (
	Hasher interface {
		Hash(b []byte) uint32
	}

	// hashers
	htCDB2a struct{}
	fnv1a   struct{}
	murmur3 struct{}
)

var (
	CDB2a   htCDB2a
	FNV1a   fnv1a
	Murmur3 murmur3
)

func (htCDB2a) Hash(b []byte) uint32 {
	h := uint32(5381)
	for _, c := range b {
		h = h<<5 + h
		h = h ^ uint32(c)
	}
	return h
}

func (fnv1a) Hash(b []byte) uint32 {
	h := uint32(2166136261)
	for _, c := range b {
		h ^= uint32(c)
		h *= 16777619
	}
	return h
}

func rot32(x, y uint32) uint32 { return ((x << y) | (x >> (32 - y))) }

func (murmur3) Hash(key []byte) uint32 {
	if len(key) == 0 {
		return 0
	}
	c1 := uint32(0xcc9e2d51)
	c2 := uint32(0x1b873593)
	r1 := uint32(15)
	r2 := uint32(13)
	n := uint32(0xe6546b64)
	hash := uint32(0)

	blocks := len(key) / 4

	buf := key
	for i := 0; i < blocks; i++ {
		k := binary.LittleEndian.Uint32(buf)
		buf = buf[4:]
		k *= c1
		k = rot32(k, r1) * c2
		hash ^= k
		hash = rot32(hash, r2)
		hash = (hash << 2) + hash + n
	}

	var k uint32
	tail := key[blocks*4:]
	switch len(tail) {
	case 3:
		k ^= uint32(tail[2]) << 16
		k ^= uint32(tail[1]) << 8
		k ^= uint32(tail[0])
	case 2:
		k ^= uint32(tail[1]) << 8
		k ^= uint32(tail[0])
	case 1:
		k ^= uint32(tail[0])
	}

	k *= c1
	k = rot32(k, r1) * c2
	hash ^= k

	hash ^= uint32(uint32(len(key)))
	hash ^= hash >> 16
	hash *= 0x85ebca6b
	hash ^= hash >> 13
	hash *= 0xc2b2ae35
	hash ^= hash >> 16
	return hash
}
