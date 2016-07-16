package cvdb

import (
	"encoding/binary"
	"fmt"
	"io"
	"os"
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
		dumpDistrib bool
	}
	cell struct {
		hash uint32
		pos  uint64
	}
	writeFile interface {
		WriteAt(b []byte, off int64) (n int, err error)
	}
)

func NewWriter(f writeFile, options *Options) (*Writer, error) {
	var opts Options
	if err := opts.check(options); err != nil {
		return nil, err
	}
	w := Writer{
		f:          f,
		numBuckets: opts.NumBuckets,
		offset:     opts.Offset,
		buckets:    make([][]cell, opts.NumBuckets),
		p:          int64(opts.NumBuckets)*12 + opts.Offset,
		hasher:     opts.Hasher,
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
	hash := w.hasher.Hash(key)
	return w.PutHash(hash, key, value)
}

// PutHash writes key and value to the stream but uses an already precomputed hash value
func (w *Writer) PutHash(hash uint32, key, value []byte) error {
	if w.err != nil {
		return w.err
	}
	const keyLen = 4
	b := cell{hash: hash, pos: uint64(w.p)}
	bucket := hash % w.numBuckets
	w.buckets[bucket] = append(w.buckets[bucket], b)
	buf := w.scratch[:8]
	binary.LittleEndian.PutUint32(buf, uint32(len(key)))
	binary.LittleEndian.PutUint32(buf[keyLen:], uint32(len(value)))
	if _, err := w.f.WriteAt(buf, w.p); err != nil {
		w.err = ioerror(err)
		return err
	}
	w.p += 8
	if _, err := w.f.WriteAt(key, w.p); err != nil {
		w.err = ioerror(err)
		return err
	}
	w.p += int64(len(key))
	if _, err := w.f.WriteAt(value, w.p); err != nil {
		w.err = ioerror(err)
		return err
	}
	w.p += int64(len(value))
	return w.err
}

// Commit() must be called to finalizes the database. It writes the seek information
// into the file
func (w *Writer) Commit() error {
	if w.err != nil {
		return w.err
	}
	buf := w.scratch[:12]
	w.committed = true
	trailer := w.p
	for i := range w.buckets {
		if len(w.buckets[i]) == 0 {
			continue
		}
		cells := make([]cell, len(w.buckets[i])*2)
		for _, b := range w.buckets[i] {
			p := int((2 * (b.hash / w.numBuckets)) % uint32(len(cells)))
			for cells[p].hash != 0 {
				p = (p + 1) % len(cells)
				w.skipped++
			}
			cells[p] = cell{hash: b.hash, pos: b.pos}
		}
		for _, c := range cells {
			binary.LittleEndian.PutUint32(buf, c.hash)
			binary.LittleEndian.PutUint64(buf[HashWidth:], c.pos)
			if _, err := w.f.WriteAt(buf[:HashWidth+8], w.p); err != nil {
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
		if nc := uint32(len(b)) * 2; nc == 0 {
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
