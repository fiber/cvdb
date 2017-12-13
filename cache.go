package cvdb

import (
	"io"
	"path/filepath"
	"sync"
	"sync/atomic"
)

type (
	Cache struct {
		entries map[string]*CacheEntry
		serial  uint64
		target  int
		sync.Mutex
	}
	CacheEntry struct {
		Reader *Reader
		*Options
		serial   uint64
		refCount int32
	}
	CReader struct {
		rd       *Reader
		e        *CacheEntry
		c        *Cache
		isClosed bool
	}
)

func NewCache(target int) *Cache {
	return &Cache{target: target, entries: make(map[string]*CacheEntry)}
}

func eqOptions(o1, o2 *Options) bool {
	if o1 == nil || o2 == nil {
		return o2 == o1
	}
	return *o2 == *o1
}

func (c *Cache) OpenOpts(fname string, opts *Options) (*CReader, error) {
	sn := atomic.AddUint64(&c.serial, 1)
	fn, err := filepath.Abs(fname)
	if err != nil {
		return nil, err
	}
	c.Lock()
	e, ok := c.entries[fn]
	if ok {
		c.Unlock()
		if !eqOptions(e.Options, opts) {
			f, err := OpenOpts(fname, opts)
			if err != nil {
				return nil, err
			}
			return &CReader{rd: f}, nil
		}
		cr := &CReader{
			c:  c,
			e:  e,
			rd: e.Reader.Clone(),
		}
		atomic.AddInt32(&e.refCount, 1)
		atomic.StoreUint64(&e.serial, sn)
		return cr, nil
	}
	for len(c.entries) > c.target {
		var (
			fd string
			ed *CacheEntry
		)
		ser := sn
		for f, e := range c.entries {
			if atomic.LoadInt32(&e.refCount) > 0 {
				continue
			}
			if s := atomic.LoadUint64(&e.serial); s < ser {
				fd = f
				ser = s
				ed = e
			}
		}
		if ed == nil {
			break
		}
		delete(c.entries, fd)
		defer ed.Reader.Close()
	}
	rd, err := OpenOpts(fn, opts)
	if err != nil {
		c.Unlock()
		return nil, err
	}
	ce := &CacheEntry{
		Reader:   rd,
		Options:  opts,
		serial:   sn,
		refCount: 1,
	}
	c.entries[fn] = ce
	cr := &CReader{
		c:  c,
		e:  ce,
		rd: ce.Reader.Clone(),
	}
	c.Unlock()
	return cr, nil
}

func (c *Cache) Invalidate(fname string) {
	fn, err := filepath.Abs(fname)
	if err != nil {
		return
	}
	c.Lock()
	e, ok := c.entries[fn]
	if !ok {
		c.Unlock()
		return
	}
	delete(c.entries, fn)
	c.Unlock()
	e.Reader.Close()
}

// Open opens a database file
func (c *Cache) Open(fname string) (*CReader, error) {
	return c.OpenOpts(fname, nil)
}

func (cr *CReader) Close() error {
	if cr==nil || cr.isClosed {
		return nil
	}
	cr.isClosed = true
	if cr.e != nil {
		atomic.AddInt32(&cr.e.refCount, -1)
	}
	return cr.rd.Close()
}

func (r *CReader) Get(key []byte) (value []byte, err error) {
	if r.e != nil {
		atomic.StoreUint64(&r.e.serial, atomic.AddUint64(&r.c.serial, 1))
	}
	return r.rd.Get(key)
}

func (r *CReader) Clone() *Reader {
	return r.rd.Clone()
}

// HasKey returns true,nil if the key is present in the DB
func (r *CReader) HasKey(key []byte) (bool, error) {
	return r.rd.HasKey(key)
}

// HasKeyHash returns true,nil if the key is present in the DB
func (r *CReader) HasKeyHash(hash uint32, key []byte) (bool, error) {
	return r.rd.HasKeyHash(hash, key)
}

// SetBuffer allows reader to use the provided buffer space. Note, that it will
// resize the slice to use it's entire capacity (not just length)
func (r *CReader) SetBuffer(b []byte) {
	r.rd.SetBuffer(b)
}

// SetBufferSize creates a buffer of at least size
func (r *CReader) SetBufferSize(size int) {
	r.rd.SetBufferSize(size)
}

func (r *CReader) GetReader(key []byte) (value io.Reader, err error) {
	return r.rd.GetReader(key)
}

func (r *CReader) GetHashReader(hash uint32, key []byte) (value io.Reader, err error) {
	return r.rd.GetHashReader(hash, key)
}

func (r *CReader) IdxIterator() *Iterator {
	return r.rd.IdxIterator()
}

func (r *CReader) Iterator() *Iterator2 {
	return r.rd.Iterator()
}
