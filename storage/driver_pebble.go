package storage

import "github.com/cockroachdb/pebble"

// PebbleDriver is the driver manager for pebble
type PebbleDriver struct {
	dbs []*PebbleDriverStorage
}

// Bucket returns a new bucket
func (pd *PebbleDriver) Bucket(name string) (Storage, error) {
	return nil, nil
}

// PebbleDriverStorage is the driver for pebble storage engine
type PebbleDriverStorage struct {
	db *pebble.DB
}

// NewPebbleDriverStorage fatories a new PebbleDriverStorage instance
func NewPebbleDriverStorage(dirname string, opts *pebble.Options) (*PebbleDriverStorage, error) {
	db, err := pebble.Open(dirname, opts)
	if err != nil {
		return nil, err
	}
	return &PebbleDriverStorage{
		db: db,
	}, nil
}

// GetDB returns the pebble DB instance
func (pds *PebbleDriverStorage) GetDB() *pebble.DB {
	return pds.db
}

// Get gets an entry from the DB
func (pds *PebbleDriverStorage) Get(key []byte) ([]byte, error) {
	dat, closer, err := pds.db.Get(key)
	defer closer.Close()
	if err != nil {
		return nil, err
	}
	return dat, nil
}

// Set puts an entry to the DB
func (pds *PebbleDriverStorage) Set(key []byte, value []byte, options *SetOptions) error {
	return pds.db.Set(key, value, &pebble.WriteOptions{
		Sync: options.Synchronized,
	})
}

// NewIter creates a new iterator
