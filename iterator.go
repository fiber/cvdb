package cvdb

import "encoding/binary"

// Iterator wraps an object
type Iterator struct {
	KeysOnly bool // you can set keysonly to true if you don't care about the values when iterating
	r        *Reader
	tbl      int
	idx      uint32
	err      error
	buf      []byte
}

func (r *Reader) Iterator() *Iterator {
	return &Iterator{r: r, buf: make([]byte, 1024)}
}

func (it *Iterator) First() ([]byte, []byte) {
	it.tbl = 0
	it.idx = 0
	return it.Next()
}

func (it *Iterator) Next() ([]byte, []byte) {
	var scratch [16]byte
again:
	if it.err != nil || it.tbl == int(it.r.numBuckets) {
		return nil, nil
	}
	currentBucket := it.r.index[it.tbl]
	for it.idx < currentBucket.len {
		fpos := int64(currentBucket.pos)
		fpos += 12 * int64(it.idx)
		if _, err := it.r.f.ReadAt(scratch[:12], fpos); err != nil {
			it.err = ioerror(err)
			return nil, nil
		}
		it.idx++
		pos := int64(binary.LittleEndian.Uint64(scratch[4:12]))
		if pos == 0 {
			continue
		}
		if _, err := it.r.f.ReadAt(scratch[:8], pos); err != nil {
			it.err = ioerror(err)
			return nil, nil
		}
		kl := binary.LittleEndian.Uint32(scratch[:4])
		var vl uint32
		if !it.KeysOnly {
			vl = binary.LittleEndian.Uint32(scratch[4:])
		}
		bl := int(kl) + int(vl)
		if cap(it.buf) < bl {
			it.buf = make([]byte, bl)
		} else {
			it.buf = it.buf[:bl]
		}
		if _, err := it.r.f.ReadAt(it.buf, pos+8); err != nil {
			it.err = ioerror(err)
			return nil, nil
		}
		return it.buf[:kl], it.buf[kl:]
	}
	it.tbl++
	it.idx = 0
	goto again
}

func (it *Iterator) Err() error {
	return it.err
}
