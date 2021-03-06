package storage

// BatchType describes types of a batch
type BatchType uint8

const (
	// BatchWriteOnly as is
	BatchWriteOnly = BatchType(1)
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
	// Set(key []byte, value []byte, options *SetOptions) error
	// Delete(key []byte) error
	// DeleteRange(start []byte, end []byte) error
	NewIter(start []byte, stop []byte) Iterator
	NewBatch(typ BatchType) Batch
}

// Batch provides a method to execute a bunch of commands
type Batch interface {
	Get(key []byte) ([]byte, error)
	Set(key []byte, value []byte, options *SetOptions) error
	Delete(key []byte) error
	DeleteRange(start []byte, end []byte) error
	NewIter(start []byte, stop []byte) Iterator
	Commit() error
}

// SetOptions provide an interface for users
// to pass options to set methods
type SetOptions struct {
	Synchronized bool
}

// Iterator is an iterable data structure
type Iterator interface {
	First() bool
	Last() bool
	Next() bool
	Prev() bool
	SeekGE(m []byte) bool
	SeekLT(m []byte) bool
	Valid() bool
	Value() []byte
	Key() []byte
}
