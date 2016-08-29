package cvdb

import (
	"errors"
	"io"
)

type (

	// IOError wraps all io errors
	IOError error
	// UserError wraps all user/parameter errors
	UserError error
	// DBError wraps all errors with the database layout
	DBError error
)

var (
	errInvalidParams      = UserError(errors.New("invalid parameters"))
	errNotCloser          = UserError(errors.New("writer does not support close method"))
	errInvalidHeader      = DBError(errors.New("invalid db header"))
	errDBCorrupt          = DBError(errors.New("database is corrupt"))
	ErrValueTooLarge      = UserError(errors.New("attempt to read a very large value from DB"))
	errInvalidKeyLength   = errors.New("invalid key length")
	errInvalidValueLength = errors.New("invalid value length")
)

func ioerror(err error) error {
	if err == io.EOF { // never wrap EOF
		return err
	}
	return IOError(err)
}

func IsIOError(err error) bool {
	_, e := err.(IOError)
	return e
}

func IsUserError(err error) bool {
	_, e := err.(UserError)
	return e
}

func IsDBError(err error) bool {
	_, e := err.(DBError)
	return e
}
