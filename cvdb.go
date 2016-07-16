package cvdb

var DefaultHasher = FNV1a

const (
	NumBuckets = 256
	HashBits   = 32
)

// find the start pos in the bucket at which this
func startPos(hash uint32, numBuckets uint32, cells int) int {
	p := uint64(hash) / uint64(numBuckets)
	p = p % uint64(cells)
	return int(p)
}

func scaledLength(l int) int {
	return l + l
}
