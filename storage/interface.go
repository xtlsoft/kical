package storage

// Storage is the driver interface
type Storage interface {
	Get(key []byte, value []byte) ([]byte, error)
	Set(key []byte, value []byte, options *SetOptions) error
	NewIter(start []byte, stop []byte) Iterator
}

// SetOptions provide an interface for users
// to pass options to set methods
type SetOptions struct {
	Synchronized bool
}

// Iterator is an iterable data structure
type Iterator interface {
}
