package cvdb

import (
	"encoding/binary"
	"fmt"
	"io"
	"math/rand"
	"os"
	"sync/atomic"

	"sync"
)

type (
	Writer struct {
		buckets     [][]cell
		hasher      Hasher
		f           writeFile
		p           int64 // current position in stream
		offset      int64 // offset to add when seeking
		committed   bool
		numBuckets  uint32
		err         error
		scratch     [16]byte
		skipped     int64
		largeValues bool // support values larger than 4GB
		dumpDistrib bool
		wLock       sync.Mutex // write ops
	}
	cell struct {
		hash uint32
		pos  uint64
	}
	writeFile interface {
		WriteAt(b []byte, off int64) (n int, err error)
		ReadAt(b []byte, off int64) (n int, err error)
	}
)

func NewWriter(f writeFile, options *Options) (*Writer, error) {
	var opts Options
	if err := opts.check(options); err != nil {
		return nil, err
	}
	w := Writer{
		f:           f,
		numBuckets:  opts.NumBuckets,
		offset:      opts.Offset,
		buckets:     make([][]cell, opts.NumBuckets),
		p:           int64(opts.NumBuckets)*12 + opts.Offset,
		hasher:      opts.Hasher,
		largeValues: opts.LargeValues,
	}
	rate := atomic.LoadUint64(&profileRate)
	if rate > 0 && (rate == 1 || rand.Int63n(int64(rate)) == 0) {
		cvdbProfile.Add(&w, 0)
	}
	return &w, nil
}

// Create creates a new file and opens it for writing
func Create(fname string) (*Writer, error) {
	return CreateOpts(fname, nil)
}

func CreateOpts(fname string, opts *Options) (*Writer, error) {
	f, err := os.Create(fname)
	if err != nil {
		return nil, ioerror(err)
	}
	w, err := NewWriter(f, opts)
	if err != nil {
		f.Close()
	}
	return w, err
}

// Put puts a key and value into the file
func (w *Writer) Put(key, value []byte) error {
	_, err := w.PutFP(key, value)
	return err
}

func (w *Writer) PutFP(key, value []byte) (FPos, error) {
	hash := w.hasher.Hash(key)
	return w.PutHashFP(hash, key, value)
}

func (w *Writer) PutReader(key []byte, valueR io.Reader) error {
	_, err := w.PutReaderFP(key, valueR)
	return err
}

func (w *Writer) PutReaderFP(key []byte, valueR io.Reader) (FPos, error) {
	hash := w.hasher.Hash(key)
	return w.PutHashReaderFP(hash, key, valueR)
}

func (w *Writer) putHashStart(hash uint32, key []byte, valuelen int) (FPos, error) {
	var fpos FPos
	if w.err != nil {
		return fpos, w.err
	}
	const keyLen = 4
	b := cell{hash: hash, pos: uint64(w.p)}
	bucket := hash % w.numBuckets
	if int(bucket) < len(w.buckets) {
		w.buckets[bucket] = append(w.buckets[bucket], b)
	} else {
		panic(fmt.Sprintf("bucket is %v, len is %v, hash is %x", bucket, len(w.buckets), hash))
	}
	var buf []byte
	if w.largeValues {
		buf = w.scratch[:12]
		binary.LittleEndian.PutUint32(buf, uint32(len(key)))
		binary.LittleEndian.PutUint64(buf[keyLen:], uint64(valuelen))
	} else {
		buf = w.scratch[:8]
		binary.LittleEndian.PutUint32(buf, uint32(len(key)))
		binary.LittleEndian.PutUint32(buf[keyLen:], uint32(valuelen))
	}
	fpos.hdrLen = len(buf)
	if _, err := w.f.WriteAt(buf, w.p); err != nil {
		w.err = ioerror(err)
		return fpos, err
	}
	w.p += int64(len(buf))
	fpos.keyPos = w.p
	if _, err := w.f.WriteAt(key, w.p); err != nil {
		w.err = ioerror(err)
		return fpos, err
	}
	w.p += int64(len(key))
	return fpos, nil
}

type FPos struct {
	hdrLen int
	keyPos int64
	valPos int64
	valLen int64
}

func (fp *FPos) ValLen() int64 {
	return fp.valLen
}
func (fp *FPos) ValPos() int64 {
	return fp.valPos
}
func (fp *FPos) KeyPos() int64 {
	return fp.keyPos
}
func (fp *FPos) KeyLen() int {
	return int(fp.valPos - fp.keyPos)
}
func (fp *FPos) HkvPos() int64 {
	return fp.keyPos - int64(fp.hdrLen)
}
func (w *Writer) ReadBack(fp FPos) io.Reader {
	rdr := posreader{r: w.f, pos: fp.valPos, len: fp.valLen}
	return &rdr
}

// PutHash writes key and value to the stream but uses an already precomputed hash value
func (w *Writer) PutHash(hash uint32, key, value []byte) error {
	_, err := w.PutHashFP(hash, key, value)
	return err
}

func (w *Writer) PutHashFP(hash uint32, key, value []byte) (FPos, error) {
	w.wLock.Lock()
	fpos, err := w.putHashStart(hash, key, len(value))
	if err != nil {
		w.wLock.Unlock()
		return fpos, err
	}
	_, err = w.f.WriteAt(value, w.p)
	w.wLock.Unlock()
	if err != nil {
		w.err = ioerror(err)
		return fpos, err
	}
	fpos.valPos = w.p
	fpos.valLen = int64(len(value))
	w.p += int64(len(value))
	return fpos, w.err
}

func (w *Writer) PutHashReader(hash uint32, key []byte, valueR io.Reader) error {
	_, err := w.PutHashReaderFP(hash, key, valueR)
	return err
}

func (w *Writer) PutHashReaderFP(hash uint32, key []byte, valueR io.Reader) (FPos, error) {
	valpos := w.p + 4
	w.wLock.Lock()
	fpos, err := w.putHashStart(hash, key, 0)
	if err != nil {
		w.wLock.Unlock()
		return fpos, err
	}
	fpos.valPos = w.p
	valLen, err := io.Copy(poswriter(w, w.p), valueR)
	fpos.valLen = valLen
	if err != nil {
		w.err = ioerror(err)
		w.wLock.Unlock()
		return fpos, w.err
	}
	w.p += valLen
	var buf []byte
	if w.largeValues {
		buf = w.scratch[:8]
		binary.LittleEndian.PutUint64(buf, uint64(valLen))
	} else {
		buf = w.scratch[:4]
		binary.LittleEndian.PutUint32(buf, uint32(valLen))
	}
	_, err = w.f.WriteAt(buf, valpos)
	w.wLock.Unlock()
	if err != nil {
		w.err = ioerror(err)
		return fpos, w.err
	}
	return fpos, w.err
}

type posWriter struct {
	w   *Writer
	pos int64
}

func poswriter(w *Writer, pos int64) *posWriter {
	return &posWriter{w: w, pos: pos}
}

func (pos *posWriter) Write(b []byte) (int, error) {
	n, err := pos.w.f.WriteAt(b, pos.pos)
	pos.pos += int64(n)
	return n, err
}

// Commit() must be called to finalizes the database. It writes the seek information
// into the file
func (w *Writer) Commit() error {
	if w.err != nil {
		return w.err
	}
	w.wLock.Lock()
	defer w.wLock.Unlock()
	buf := w.scratch[:12]
	w.committed = true
	trailer := w.p
	for i, wi := range w.buckets {
		if len(wi) == 0 {
			continue
		}
		cells := make([]cell, scaledLength(len(wi)))
		for _, b := range wi {
			p := startPos(b.hash, w.numBuckets, len(cells))
			for cells[p].hash != 0 {
				p = (p + 1) % len(cells)
				w.skipped++
			}
			cells[p] = cell{hash: b.hash, pos: b.pos}
		}
		for _, c := range cells {
			binary.LittleEndian.PutUint32(buf, c.hash)
			binary.LittleEndian.PutUint64(buf[HashBits/8:], c.pos)
			if _, err := w.f.WriteAt(buf[:HashBits/8+8], w.p); err != nil {
				w.err = ioerror(err)
				break
			}
			w.p += 12
		}
		if w.dumpDistrib {
			s := ""
			for _, c := range cells {
				if c.pos == 0 {
					s += "-"
				} else {
					s += "+"
				}
			}
			fmt.Printf("[%04v] %v\n", i, s)
		}
	}
	if w.err != nil {
		return w.err
	}
	// update the header
	bp := w.offset
	for _, b := range w.buckets {
		if nc := uint32(scaledLength(len(b))); nc == 0 {
			for i := range buf[:12] {
				buf[i] = 0
			}
		} else {
			binary.LittleEndian.PutUint64(buf, uint64(trailer))
			binary.LittleEndian.PutUint32(buf[8:], nc)
			trailer += int64(nc) * (4 + 8)
		}
		if _, err := w.f.WriteAt(buf[:4+8], bp); err != nil {
			w.err = ioerror(err)
			break
		}
		bp += 12
	}
	return w.err
}

// Close closes the underlying file. If the database was not committed, it will
// attempt to commit the database before closing the file
// It is conventional to call Commit() explicitly before Close()
// Close should not be called, when the underlying writer does not support Close()
func (w *Writer) Close() error {
	cvdbProfile.Remove(w)
	cl, isCloser := w.f.(io.Closer)
	if !w.committed {
		if err := w.Commit(); err != nil {
			if isCloser {
				cl.Close()
			}
			w.buckets = nil
			return err
		}
	}
	w.buckets = nil
	if isCloser {
		return cl.Close()
	}
	return errNotCloser
}
