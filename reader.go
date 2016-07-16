package cvdb

import (
	"bytes"
	"encoding/binary"
	"io"
	"os"
)

type (
	Reader struct {
		index      []hentry
		offset     int64
		numBuckets uint32
		hasher     Hasher
		scratch    [16]byte
		f          readFile
		buf        []byte
		cloned     bool
	}
	hentry struct {
		pos uint64
		len uint32
	}
	readFile interface {
		ReadAt(b []byte, off int64) (n int, err error)
	}
)

// Clone clones a reader, so that it can be used by another goroutine. The main
// difference is that the new Reader will have it's own buffer space. You may
// want to use SetBuffer or SetBufferSize to size the buffer appropriately
func (r *Reader) Clone() *Reader {
	r2 := Reader{
		index:      r.index,
		offset:     r.offset,
		numBuckets: r.numBuckets,
		hasher:     r.hasher,
		f:          r.f,
		cloned:     true,
	}
	return &r2
}

// NewReader initializes a new reader, and reads the DB header
func NewReader(f readFile, options *Options) (*Reader, error) {
	var opts Options
	if err := opts.check(options); err != nil {
		return nil, err
	}
	r := Reader{
		f:          f,
		offset:     opts.Offset,
		numBuckets: opts.NumBuckets,
		hasher:     opts.Hasher,
	}
	r.index = make([]hentry, opts.NumBuckets)
	buf := make([]byte, opts.NumBuckets*12)
	n, err := f.ReadAt(buf, opts.Offset)
	if err != nil {
		return nil, ioerror(err)
	}
	if n != int(opts.NumBuckets)*12 {
		return nil, errInvalidHeader
	}
	for i := range r.index {
		p := i * 12
		r.index[i] = hentry{
			pos: binary.LittleEndian.Uint64(buf[p:]),
			len: binary.LittleEndian.Uint32(buf[p+8:]),
		}
	}
	return &r, nil
}

// Open opens a database file
func Open(fname string) (*Reader, error) {
	return OpenOpts(fname, nil)
}

func OpenOpts(fname string, opts *Options) (*Reader, error) {
	f, err := os.Open(fname)
	if err != nil {
		return nil, ioerror(err)
	}
	r, err := NewReader(f, opts)
	if err != nil {
		f.Close()
	}
	return r, err
}

// Get returns the value for key. If the key is not present, nil will returned.
// it the key is present with no data, an empty slice is returned. Get will return an
// error as it sees fit.
func (r *Reader) Get(key []byte) (value []byte, err error) {
	hash := r.hasher.Hash(key)
	return r.GetHash(hash, key)
}

type kvloc struct {
	pos int64
	kl  int
	vl  int
}

// findKey returns a kvloc object for the given key or an error.
// if the key is not present in the db, the pos field will be 0, but no
// error is returned
func (r *Reader) findKey(hash uint32, key []byte) (kvloc, error) {
	var kv kvloc
	var scratch [16]byte
	bucket := hash % r.numBuckets
	length := int(r.index[bucket].len)
	if length == 0 {
		return kv, nil
	}
	fpos := int64(r.index[bucket].pos)
	start := int((2 * (hash / r.numBuckets)) % uint32(length))
	for i := 0; i < length; i++ {
		spos := fpos + int64((start+i)%length)*12
		if _, err := r.f.ReadAt(scratch[:12], spos); err != nil {
			return kv, ioerror(err)
		}
		pos := int64(binary.LittleEndian.Uint64(scratch[4:12]))
		if pos == 0 {
			return kv, nil
		}
		hash2 := binary.LittleEndian.Uint32(scratch[:4])
		if hash2 != hash {
			continue
		}
		if _, err := r.f.ReadAt(scratch[:8], pos); err != nil {
			return kv, ioerror(err)
		}
		keyLen := int(binary.LittleEndian.Uint32(scratch[:4]))
		if keyLen != len(key) {
			continue
		}
		valLen := int(binary.LittleEndian.Uint32(scratch[4:8]))
		s := keyLen
		if valLen > s {
			s = valLen
		}
		r.grow(s)
		if _, err := r.f.ReadAt(r.buf[:keyLen], pos+8); err != nil {
			return kv, ioerror(err)
		}
		if !bytes.Equal(key, r.buf[:keyLen]) {
			continue
		}
		kv.pos = pos
		kv.kl = keyLen
		kv.vl = valLen
		return kv, nil
	}
	return kv, errDBCorrupt
}

// HasKey returns true,nil if the key is present in the DB
func (r *Reader) HasKey(key []byte) (bool, error) {
	hash := r.hasher.Hash(key)
	return r.HasKeyHash(hash, key)
}

// HasKeyHash returns true,nil if the key is present in the DB
func (r *Reader) HasKeyHash(hash uint32, key []byte) (bool, error) {
	kv, err := r.findKey(hash, key)
	if err != nil {
		return false, err
	}
	return kv.pos != 0, nil
}

// SetBuffer allows reader to use the provided buffer space. Note, that it will
// resize the slice to use it's entire capacity (not just length)
func (r *Reader) SetBuffer(b []byte) {
	r.buf = b
}

// SetBufferSize creates a buffer of at least size
func (r *Reader) SetBufferSize(size int) {
	if cap(r.buf) > 2*size {
		r.buf = nil
	}
	r.grow(size)
}

// set internal buffer to at least size
func (r *Reader) grow(size int) {
	if size < 0 {
		return
	}
	if cap(r.buf) < size {
		bs := size
		bs = bs + 257 - (bs-1)%256 // size to next multiple of 256 bytes
		r.buf = make([]byte, bs)
	}
	r.buf = r.buf[:size]
}

// GetHash returns the value for key or (nil, err) when an error occurs. It is
// there for tools that already have the key's hash value precomputed
func (r *Reader) GetHash(hash uint32, key []byte) (value []byte, err error) {
	kv, err := r.findKey(hash, key)
	if err != nil {
		return nil, err
	}
	if kv.pos == 0 {
		return nil, nil
	}
	r.grow(kv.vl)
	if _, err := r.f.ReadAt(r.buf[:kv.vl], kv.pos+8+int64(kv.kl)); err != nil {
		return nil, ioerror(err)
	}
	return r.buf[:kv.vl], nil
}

// Close closes the underlying file. If the database was not committed, it will
// attempt to commit the database before closing the file
// It is conventional to call Commit() explicitly before Close()
// Close should not be called, when the underlying writer does not support Close()
func (r *Reader) Close() error {
	cl, isCloser := r.f.(io.Closer)
	if isCloser {
		return cl.Close()
	}
	return errNotCloser
}
