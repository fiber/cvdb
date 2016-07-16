package cvdb

import "errors"

type (

	// IOError wraps all io errors
	IOError error
	// UserError wraps all user/parameter errors
	UserError error
	// DBError wraps all errors with the database layout
	DBError error
)

var (
	errInvalidParams = UserError(errors.New("invalid parameters"))
	errNotCloser     = UserError(errors.New("writer does not support close method"))
	errInvalidHeader = DBError(errors.New("invalid db header"))
	errDBCorrupt     = DBError(errors.New("database is corrupt"))
)

func ioerror(err error) error {
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
