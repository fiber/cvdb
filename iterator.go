package cvdb

import "encoding/binary"

// Iterator wraps an object
type Iterator struct {
	KeysOnly bool // you can set keysonly to true if you don't care about the values when iterating
	r        *Reader
	tbl      int
	idx      uint32
	err      error
	//
	pos  int64
	hash uint32
	kl   int
	vl   int
}

// Iterator allows to scan all key-value pairs in the database.
func (r *Reader) Iterator() *Iterator {
	return &Iterator{r: r.Clone()}
}

// Next will advance to and read the next key-value pair from the DB and return true.
// it returns false, if there are no more keys or an error occured, in which case
// Err() will return a non-nil error
func (it *Iterator) Next() bool {
	var scratch [16]byte
	for {
		if it.err != nil || it.tbl == int(it.r.numBuckets) {
			return false
		}
		currentBucket := it.r.index[it.tbl]
		for it.idx < currentBucket.len {
			fpos := int64(currentBucket.pos)
			fpos += 12 * int64(it.idx)
			if _, err := it.r.f.ReadAt(scratch[:12], fpos); err != nil {
				it.err = ioerror(err)
				return false
			}
			it.idx++
			it.hash = binary.LittleEndian.Uint32(scratch[:4])
			it.pos = int64(binary.LittleEndian.Uint64(scratch[4:12]))
			if it.pos == 0 {
				continue
			}
			if _, err := it.r.f.ReadAt(scratch[:8], it.pos); err != nil {
				it.err = ioerror(err)
				return false
			}
			kl := binary.LittleEndian.Uint32(scratch[:4])
			var vl uint32
			if !it.KeysOnly {
				vl = binary.LittleEndian.Uint32(scratch[4:])
			}
			bl := int(kl) + int(vl)
			it.r.grow(bl)
			if _, err := it.r.f.ReadAt(it.r.buf, it.pos+8); err != nil {
				it.err = ioerror(err)
				return false
			}
			it.kl = int(kl)
			it.vl = int(vl)
			return true
		}
		it.tbl++
		it.idx = 0
	}
}

func (it *Iterator) Hash() uint32 {
	return it.hash
}

func (it *Iterator) Key() []byte {
	return it.r.buf[:it.kl]
}

func (it *Iterator) Value() []byte {
	return it.r.buf[it.kl:]
}

func (it *Iterator) Err() error {
	return it.err
}
