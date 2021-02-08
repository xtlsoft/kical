package storage

import (
	"path/filepath"
	"sync"

	"github.com/cockroachdb/pebble"
)

// PebbleDriverConfigure is the configure structure of pebble
// driver
type PebbleDriverConfigure struct {
	BaseDirectory string
}

// PebbleDriver is the driver manager for pebble
type PebbleDriver struct {
	dbs     map[string]*PebbleDriverStorage
	dbsLock *sync.Mutex
	conf    *PebbleDriverConfigure
}

// NewPebbleDriver creates a new pebble driver
func NewPebbleDriver(conf *PebbleDriverConfigure) *PebbleDriver {
	return &PebbleDriver{
		conf: conf,
	}
}

// Bucket returns a new bucket
func (pd *PebbleDriver) Bucket(name string) (Storage, error) {
	pd.dbsLock.Lock()
	defer pd.dbsLock.Unlock()
	var pds *PebbleDriverStorage
	var ok bool
	if pds, ok = pd.dbs[name]; ok {
		return pds, nil
	}
	dirname := filepath.Join(pd.conf.BaseDirectory, name)
	db, err := pebble.Open(dirname, &pebble.Options{})
	if err != nil {
		return nil, err
	}
	pds = &PebbleDriverStorage{db: db}
	pd.dbs[name] = pds
	return pds, nil
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
func (pds *PebbleDriverStorage) NewIter(start []byte, stop []byte) Iterator {
	iter := pds.db.NewIter(&pebble.IterOptions{
		LowerBound: start,
		UpperBound: stop,
	})
	_ = iter
	return nil
}

type PebbleDriverIterator struct {
}
