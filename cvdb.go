package cvdb

const (
	NumBuckets  = 256
	TableMask   = 0xff
	HashWidth   = 4
	ScaleFactor = 2
)

// find the start pos in the bucket at which this
func startPos(hash uint32, numBuckets uint32, cells int) int {
	return int((ScaleFactor * (hash / numBuckets)) % uint32(cells))
}
