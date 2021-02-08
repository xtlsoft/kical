package storage

// BatchType describes types of a batch
type BatchType uint8

const (
	// BatchReadOnly as is
	BatchReadOnly = BatchType(1)
	// BatchReadWrite as is
	BatchReadWrite = BatchType(2)
)

// Driver defines a storage driver
type Driver interface {
	Bucket(name string) (Storage, error)
}

// Storage is the driver storage class interface
type Storage interface {
	Get(key []byte) ([]byte, error)
	Set(key []byte, value []byte, options *SetOptions) error
	NewIter(start []byte, stop []byte) Iterator
	NewBatch(typ BatchType) Batch
}

// Batch provides a method to execute a bunch of commands
type Batch interface {
	Get(key []byte) ([]byte, error)
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
