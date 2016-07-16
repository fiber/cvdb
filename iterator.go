package cvdb

import (
	"bytes"
	"encoding/binary"
	"io"
)

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
	vl   int64
	lazy bool
}

// Iterator allows to scan all key-value pairs in the database.
func (r *Reader) Iterator() *Iterator {
	return &Iterator{r: r.Clone()}
}

func (it *Iterator) next(lazy bool) bool {
	it.lazy = lazy
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
			bs := 8
			if it.r.largeValues {
				bs = 12
			}
			if _, err := it.r.f.ReadAt(scratch[:bs], it.pos); err != nil {
				it.err = ioerror(err)
				return false
			}
			kl := binary.LittleEndian.Uint32(scratch[:4])
			var vl uint64
			if !it.KeysOnly {
				if bs == 8 {
					vl = uint64(binary.LittleEndian.Uint32(scratch[4:]))
				} else {
					vl = binary.LittleEndian.Uint64(scratch[4:])
				}
			}
			it.kl = int(kl)
			it.vl = int64(vl)
			if lazy {
				if vl < 128*1024 {
					it.r.grow(int(kl) + int(vl))
					it.lazy = false
				} else {
					it.r.grow(int(kl))
				}
			} else {
				if vl > 1<<31 {
					it.err = ErrValueTooLarge
					return false
				}
				it.r.grow(int(kl) + int(vl))
			}
			if _, err := it.r.f.ReadAt(it.r.buf, it.pos+int64(bs)); err != nil {
				it.err = ioerror(err)
				return false
			}
			return true
		}
		it.tbl++
		it.idx = 0
	}
}

// Next will advance to and read the next key-value pair from the DB and return true.
// it returns false, if there are no more keys or an error occured, in which case
// Err() will return a non-nil error
// Err will return ErrValueTooLarge when value is larger than 2^31 bytes. You should
// use NextReader and ValueReader instead
func (it *Iterator) Next() bool {
	return it.next(false)
}

func (it *Iterator) NextReader() bool {
	return it.next(true)
}

func (it *Iterator) Hash() uint32 {
	return it.hash
}

func (it *Iterator) Key() []byte {
	return it.r.buf[:it.kl]
}

func (it *Iterator) Value() []byte {
	if !it.lazy {
		return it.r.buf[it.kl:]
	}
	var b bytes.Buffer
	if _, err := io.Copy(&b, it.ValueReader()); err != nil {
		return nil
	}
	return b.Bytes()
}

func (it *Iterator) ValueReader() io.Reader {
	off := int64(8)
	if it.r.largeValues {
		off = 12
	}
	return posReader(it.r, it.pos+off+int64(it.kl), it.vl)
}

func (it *Iterator) Err() error {
	return it.err
}
