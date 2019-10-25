package cvdb

import (
	"bytes"
	"encoding/binary"
	"io"
	"math/rand"
	"os"
	"runtime/pprof"
	"sync/atomic"
)

type (
	Reader struct {
		index       []hentry
		offset      int64
		numBuckets  uint32
		hasher      Hasher
		scratch     [16]byte
		f           readFile
		buf         []byte
		cloned      bool
		largeValues bool
		refCount    *int32
		isClosed    bool
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
		index:       r.index,
		offset:      r.offset,
		numBuckets:  r.numBuckets,
		hasher:      r.hasher,
		f:           r.f,
		cloned:      true,
		largeValues: r.largeValues,
		refCount:    r.refCount,
	}
	atomic.AddInt32(r2.refCount, 1)
	return &r2
}

// NewReader initializes a new reader, and reads the DB header
func NewReader(f readFile, options *Options) (*Reader, error) {
	var opts Options
	if err := opts.check(options); err != nil {
		return nil, err

	}
	ref := new(int32)
	*ref = 1
	r := Reader{
		f:           f,
		offset:      opts.Offset,
		numBuckets:  opts.NumBuckets,
		hasher:      opts.Hasher,
		largeValues: opts.LargeValues,
		refCount:    ref,
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
	rate := atomic.LoadUint64(&profileRate)
	if rate > 0 && (rate == 1 || rand.Int63n(int64(rate)) == 0) {
		cvdbProfile.Add(&r, 0)
	}
	return &r, nil
}

// Open opens a database file
func Open(fname string) (*Reader, error) {
	return OpenOpts(fname, nil)
}

var (
	cvdbProfile = pprof.NewProfile("cvdb")
	profileRate uint64
)

func SetProfileRate(rate int) int {
	old := int(atomic.LoadUint64(&profileRate))
	if rate >= 0 {
		atomic.StoreUint64(&profileRate, uint64(rate))
	}
	return old
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
	vl  int64
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
	start := startPos(hash, r.numBuckets, length)
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
		bs := 8
		if r.largeValues {
			bs = 12
		}
		if _, err := r.f.ReadAt(scratch[:bs], pos); err != nil {
			return kv, ioerror(err)
		}
		keyLen := int(binary.LittleEndian.Uint32(scratch[:4]))
		if keyLen != len(key) {
			continue
		}

		var valLen int64
		if r.largeValues {
			valLen = int64(binary.LittleEndian.Uint64(scratch[4:12]))
		} else {
			valLen = int64(binary.LittleEndian.Uint32(scratch[4:8]))
		}
		s := keyLen
		r.scale(s)
		if _, err := r.f.ReadAt(r.buf[:keyLen], pos+int64(bs)); err != nil {
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
	r.scale(size)
}

// set internal buffer to at least size
func (r *Reader) scale(size int) (moved bool) {
	if size < 0 {
		return
	}
	if cap(r.buf) < size {
		bs := size
		bs = bs + 257 - (bs-1)%256 // size to next multiple of 256 bytes
		r.buf = make([]byte, bs)
		moved = true
	}
	r.buf = r.buf[:size]
	return
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
	if kv.vl >= (1<<32 - 1) {
		return nil, ErrValueTooLarge
	}
	bs := int(kv.vl)
	r.scale(bs)
	ss := int64(8)
	if r.largeValues {
		ss = 12
	}
	if _, err := r.f.ReadAt(r.buf[:bs], kv.pos+ss+int64(kv.kl)); err != nil {
		return nil, ioerror(err)
	}
	return r.buf[:bs], nil
}

func (r *Reader) GetReader(key []byte) (value io.Reader, err error) {
	hash := r.hasher.Hash(key)
	return r.GetHashReader(hash, key)
}

func (r *Reader) GetHashReader(hash uint32, key []byte) (value io.Reader, err error) {
	kv, err := r.findKey(hash, key)
	if err != nil {
		return nil, err
	}
	if kv.pos == 0 {
		return nil, nil
	}
	return posReader(r, kv.pos, kv.vl), nil
}

type ReadAter interface {
	ReadAt(b []byte, off int64) (n int, err error)
}
type posreader struct {
	r   ReadAter
	pos int64
	len int64
}

func posReader(r *Reader, pos int64, len int64) *posreader {
	return &posreader{r: r.f, pos: pos, len: len}
}

func (r *posreader) Read(b []byte) (int, error) {
	if r.len == 0 {
		return 0, io.EOF
	}
	m := len(b)
	if r.len < int64(m) {
		b = b[:int(r.len)]
	}
	n, err := r.r.ReadAt(b, r.pos)
	r.pos += int64(n)
	r.len -= int64(n)
	return n, err
}

// Close closes the underlying file.
// It is conventional to call Commit() explicitly before Close()
// Close should not be called, when the underlying reader does not support Close()
func (r *Reader) Close() error {
	if r.isClosed {
		return nil
	}
	cvdbProfile.Remove(r.f)

	if atomic.AddInt32(r.refCount, -1) > 0 {
		return nil
	}
	cl, isCloser := r.f.(io.Closer)
	if isCloser {
		return cl.Close()
	}
	return errNotCloser
}
