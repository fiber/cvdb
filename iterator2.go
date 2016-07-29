package cvdb

import (
	"bytes"
	"encoding/binary"
	"io"
)

// Iterator2 wraps an iterator.
type Iterator2 struct {
	KeysOnly bool // you can set keysonly to true if you don't care about the values when iterating
	r        *Reader
	err      error
	pos      int64
	ep       int64
	//
	kl   int
	vl   int64
	lazy bool
}

// Iterator allows to iterate over all key-value pairs in the database. It returns key-value pairs
// in the order they where added to the database.
func (r *Reader) Iterator() *Iterator2 {
	ep := uint64(0)
	for _, b := range r.index {
		if ep == 0 || b.pos < ep {
			ep = b.pos
		}
	}
	return &Iterator2{r: r.Clone(), pos: r.offset + int64(r.numBuckets)*12, ep: int64(ep)}
}

func (it *Iterator2) next(lazy bool) bool {
	it.lazy = lazy
	var scratch [16]byte
	if it.kl > 0 && it.vl > 0 {
		if it.r.largeValues {
			it.pos += 12 + int64(it.kl) + it.vl
		} else {
			it.pos += 8 + int64(it.kl) + it.vl
		}
	}
	if it.pos >= it.ep {
		return false
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
	if bs == 8 {
		vl = uint64(binary.LittleEndian.Uint32(scratch[4:]))
	} else {
		vl = binary.LittleEndian.Uint64(scratch[4:])
	}
	it.kl = int(kl)
	it.vl = int64(vl)
	if lazy {
		if vl < 2*1024 {
			it.r.scale(int(kl) + int(vl))
			it.lazy = false
		} else {
			it.r.scale(int(kl))
		}
	} else {
		if vl > 1<<31 {
			it.err = ErrValueTooLarge
			return false
		}
		if !it.KeysOnly {
			it.r.scale(int(kl) + int(vl))
		} else {
			it.r.scale(int(kl))
		}
	}
	if _, err := it.r.f.ReadAt(it.r.buf, it.pos+int64(bs)); err != nil {
		it.err = ioerror(err)
		return false
	}
	return true
}

// Next will advance to and read the next key-value pair from the DB and return true.
// it returns false, if there are no more keys or an error occured, in which case
// Err() will return a non-nil error
// Err will return ErrValueTooLarge when value is larger than 2^31 bytes. You should
// use NextReader and ValueReader instead
func (it *Iterator2) Next() bool {
	return it.next(false)
}

func (it *Iterator2) NextReader() bool {
	return it.next(true)
}

func (it *Iterator2) Key() []byte {
	return it.r.buf[:it.kl]
}

func (it *Iterator2) Value() []byte {
	if !it.lazy {
		return it.r.buf[it.kl:]
	}
	var b bytes.Buffer
	if _, err := io.Copy(&b, it.ValueReader()); err != nil {
		return nil
	}
	return b.Bytes()
}

func (it *Iterator2) ValueReader() io.Reader {
	off := int64(8)
	if it.r.largeValues {
		off = 12
	}
	return posReader(it.r, it.pos+off+int64(it.kl), it.vl)
}

func (it *Iterator2) Err() error {
	return it.err
}
