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
	return &PebbleDriverIterator{
		it: iter,
	}
}

// NewBatch creates a new batch
func (pds *PebbleDriverStorage) NewBatch(typ BatchType) Batch {
	switch typ {
	case BatchReadOnly:
		return &PebbleDriverBatch{
			batch: pds.db.NewBatch(),
		}
	case BatchReadWrite:
		return &PebbleDriverBatch{
			batch: pds.db.NewIndexedBatch(),
		}
	default:
		panic("Unknown argument when calling NewBatch()")
	}
}

// PebbleDriverBatch as is
type PebbleDriverBatch struct {
	batch *pebble.Batch
}

// Get gets an entry from the DB
func (pdb *PebbleDriverBatch) Get(key []byte) ([]byte, error) {
	dat, closer, err := pdb.batch.Get(key)
	defer closer.Close()
	if err != nil {
		return nil, err
	}
	return dat, nil
}

// Set puts an entry to the DB
func (pdb *PebbleDriverBatch) Set(key []byte, value []byte, options *SetOptions) error {
	return pdb.batch.Set(key, value, &pebble.WriteOptions{
		Sync: options.Synchronized,
	})
}

// NewIter creates a new iterator
func (pdb *PebbleDriverBatch) NewIter(start []byte, stop []byte) Iterator {
	iter := pdb.batch.NewIter(&pebble.IterOptions{
		LowerBound: start,
		UpperBound: stop,
	})
	return &PebbleDriverIterator{
		it: iter,
	}
}

// Commit commits a batch
func (pdb *PebbleDriverBatch) Commit() error {
	return pdb.Commit()
}

// PebbleDriverIterator as is
type PebbleDriverIterator struct {
	it *pebble.Iterator
}

// First as is
func (pdi *PebbleDriverIterator) First() bool {
	return pdi.it.First()
}

// Next as is
func (pdi *PebbleDriverIterator) Next() bool {
	return pdi.it.Next()
}

// Valid as is
func (pdi *PebbleDriverIterator) Valid() bool {
	return pdi.it.Valid()
}

// Value as is
func (pdi *PebbleDriverIterator) Value() []byte {
	return pdi.it.Value()
}

// Last as is
func (pdi *PebbleDriverIterator) Last() bool {
	return pdi.it.Last()
}

// Prev as is
func (pdi *PebbleDriverIterator) Prev() bool {
	return pdi.it.Prev()
}

// SeekGE seeks the first key greater than or equal to m
func (pdi *PebbleDriverIterator) SeekGE(m []byte) bool {
	return pdi.it.SeekGE(m)
}

// SeekLT seeks the first key less than m
func (pdi *PebbleDriverIterator) SeekLT(m []byte) bool {
	return pdi.it.SeekLT(m)
}
