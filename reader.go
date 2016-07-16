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
	}
	hentry struct {
		pos uint64
		len uint32
	}
	readFile interface {
		ReadAt(b []byte, off int64) (n int, err error)
	}
)

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

func (r *Reader) Get(key []byte) (value []byte, err error) {
	hash := r.hasher.Hash(key)
	return r.GetHashBuffer(hash, key, nil)
}

func (r *Reader) GetBuffer(key []byte, buf []byte) (value []byte, err error) {
	hash := r.hasher.Hash(key)
	return r.GetHashBuffer(hash, key, buf)
}

// GetHash returns nil,nil if the key is not found
func (r *Reader) GetHash(hash uint32, key []byte) (value []byte, err error) {
	return r.GetHashBuffer(hash, key, nil)
}

func (r *Reader) GetHashBuffer(hash uint32, key []byte, buf []byte) (value []byte, err error) {
	var scratch [16]byte
	bucket := hash % r.numBuckets
	length := int(r.index[bucket].len)
	if length == 0 {
		return nil, nil
	}
	fpos := int64(r.index[bucket].pos)
	start := int((2 * (hash / r.numBuckets)) % uint32(length))
	for i := 0; i < length; i++ {
		spos := fpos + int64((start+i)%length)*12
		if _, err := r.f.ReadAt(scratch[:12], spos); err != nil {
			return nil, ioerror(err)
		}
		pos := int64(binary.LittleEndian.Uint64(scratch[4:12]))
		if pos == 0 {
			return nil, nil
		}
		hash2 := binary.LittleEndian.Uint32(scratch[:4])
		if hash2 != hash {
			continue
		}
		if _, err := r.f.ReadAt(scratch[:8], pos); err != nil {
			return nil, ioerror(err)
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
		if len(buf) >= s {
			buf = buf[:s]
		} else {
			buf = make([]byte, s)
		}
		if _, err := r.f.ReadAt(buf[:keyLen], pos+8); err != nil {
			return nil, ioerror(err)
		}
		if !bytes.Equal(key, buf[:keyLen]) {
			continue
		}
		if _, err := r.f.ReadAt(buf[:valLen], pos+8+int64(keyLen)); err != nil {
			return nil, ioerror(err)
		}
		return buf[:valLen], nil
	}
	return nil, errDBCorrupt
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
