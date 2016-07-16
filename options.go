package cvdb

type (
	Options struct {
		NumBuckets uint32
		Offset     int64
		Hasher     Hasher
	}
)

func (opts *Options) check(seed *Options) error {
	if seed != nil {
		*opts = *seed
	}
	if opts.Offset < 0 {
		return errInvalidParams
	}
	if opts.NumBuckets == 0 {
		opts.NumBuckets = NumBuckets
	} else {
		ok := false
		for i := uint(4); i <= 16; i++ {
			if opts.NumBuckets == uint32(1<<i) {
				ok = true
			}
		}
		if !ok {
			return errInvalidParams
		}
	}
	if opts.Hasher == nil {
		opts.Hasher = DefaultHasher
	}
	return nil
}
