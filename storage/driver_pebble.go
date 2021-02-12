package storage

import (
	"path/filepath"
	"sync"
	"time"

	"github.com/cockroachdb/pebble"
	"github.com/cockroachdb/pebble/vfs"
)

// PebbleDriverConfigure is the configure structure of pebble
// driver
type PebbleDriverConfigure struct {
	BaseDirectory string
	UseMemory     bool

	// Sync sstables periodically in order to smooth out writes to disk. This
	// option does not provide any persistency guarantee, but is used to avoid
	// latency spikes if the OS automatically decides to write out a large chunk
	// of dirty filesystem buffers. This option only controls SSTable syncs; WAL
	// syncs are controlled by WALBytesPerSync.
	//
	// The default value is 512KB.
	BytesPerSync int

	// Disable the write-ahead log (WAL). Disabling the write-ahead log prohibits
	// crash recovery, but can improve performance if crash recovery is not
	// needed (e.g. when only temporary state is being stored in the database).
	//
	// TODO(peter): untested
	DisableWAL bool

	// ErrorIfExists is whether it is an error if the database already exists.
	//
	// The default value is false.
	ErrorIfExists bool

	// ErrorIfNotExists is whether it is an error if the database does not
	// already exist.
	//
	// The default value is false which will cause a database to be created if it
	// does not already exist.
	ErrorIfNotExists bool

	// The amount of L0 read-amplification necessary to trigger an L0 compaction.
	L0CompactionThreshold int

	// Hard limit on L0 read-amplification. Writes are stopped when this
	// threshold is reached. If Experimental.L0SublevelCompactions is enabled
	// this threshold is measured against the number of L0 sublevels. Otherwise
	// it is measured against the number of files in L0.
	L0StopWritesThreshold int

	// The maximum number of bytes for LBase. The base level is the level which
	// L0 is compacted into. The base level is determined dynamically based on
	// the existing data in the LSM. The maximum number of bytes for other levels
	// is computed dynamically based on the base level's maximum size. When the
	// maximum number of bytes for a level is exceeded, compaction is requested.
	LBaseMaxBytes int64

	// MaxManifestFileSize is the maximum size the MANIFEST file is allowed to
	// become. When the MANIFEST exceeds this size it is rolled over and a new
	// MANIFEST is created.
	MaxManifestFileSize int64

	// MaxOpenFiles is a soft limit on the number of open files that can be
	// used by the DB.
	//
	// The default value is 1000.
	MaxOpenFiles int

	// The size of a MemTable in steady state. The actual MemTable size starts at
	// min(256KB, MemTableSize) and doubles for each subsequent MemTable up to
	// MemTableSize. This reduces the memory pressure caused by MemTables for
	// short lived (test) DB instances. Note that more than one MemTable can be
	// in existence since flushing a MemTable involves creating a new one and
	// writing the contents of the old one in the
	// background. MemTableStopWritesThreshold places a hard limit on the size of
	// the queued MemTables.
	MemTableSize int

	// Hard limit on the size of queued of MemTables. Writes are stopped when the
	// sum of the queued memtable sizes exceeds
	// MemTableStopWritesThreshold*MemTableSize. This value should be at least 2
	// or writes will stop whenever a MemTable is being flushed.
	MemTableStopWritesThreshold int

	// MaxConcurrentCompactions specifies the maximum number of concurrent
	// compactions. The default is 1. Concurrent compactions are only performed
	// when L0 read-amplification passes the L0CompactionConcurrency threshold.
	MaxConcurrentCompactions int

	// ReadOnly indicates that the DB should be opened in read-only mode. Writes
	// to the DB will return an error, background compactions are disabled, and
	// the flush that normally occurs after replaying the WAL at startup is
	// disabled.
	ReadOnly bool

	// WALBytesPerSync sets the number of bytes to write to a WAL before calling
	// Sync on it in the background. Just like with BytesPerSync above, this
	// helps smooth out disk write latencies, and avoids cases where the OS
	// writes a lot of buffered data to disk at once. However, this is less
	// necessary with WALs, as many write operations already pass in
	// Sync = true.
	//
	// The default value is 0, i.e. no background syncing. This matches the
	// default behaviour in RocksDB.
	WALBytesPerSync int

	// WALDir specifies the directory to store write-ahead logs (WALs) in. If
	// empty (the default), WALs will be stored in the same directory as sstables
	// (i.e. the directory passed to pebble.Open).
	WALDir string

	// WALMinSyncInterval is the minimum duration between syncs of the WAL. If
	// WAL syncs are requested faster than this interval, they will be
	// artificially delayed. Introducing a small artificial delay (500us) between
	// WAL syncs can allow more operations to arrive and reduce IO operations
	// while having a minimal impact on throughput. This option is supplied as a
	// closure in order to allow the value to be changed dynamically. The default
	// value is 0.
	//
	// TODO(peter): rather than a closure, should there be another mechanism for
	// changing options dynamically?
	WALMinSyncInterval func() time.Duration
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
		dbs:     make(map[string]*PebbleDriverStorage),
		dbsLock: new(sync.Mutex),
		conf:    conf,
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
	opts := &pebble.Options{
		BytesPerSync:                pd.conf.BytesPerSync,
		DisableWAL:                  pd.conf.DisableWAL,
		ErrorIfExists:               pd.conf.ErrorIfExists,
		ErrorIfNotExists:            pd.conf.ErrorIfNotExists,
		L0CompactionThreshold:       pd.conf.L0CompactionThreshold,
		L0StopWritesThreshold:       pd.conf.L0StopWritesThreshold,
		LBaseMaxBytes:               pd.conf.LBaseMaxBytes,
		MaxManifestFileSize:         pd.conf.MaxManifestFileSize,
		MaxOpenFiles:                pd.conf.MaxOpenFiles,
		MemTableSize:                pd.conf.MemTableSize,
		MaxConcurrentCompactions:    pd.conf.MaxConcurrentCompactions,
		MemTableStopWritesThreshold: pd.conf.MemTableStopWritesThreshold,
		ReadOnly:                    pd.conf.ReadOnly,
		WALBytesPerSync:             pd.conf.WALBytesPerSync,
		WALDir:                      pd.conf.WALDir,
		WALMinSyncInterval:          pd.conf.WALMinSyncInterval,
	}
	if pd.conf.UseMemory {
		opts.FS = vfs.NewMem()
	}
	db, err := pebble.Open(dirname, opts)
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
	if err != nil {
		if err == pebble.ErrNotFound {
			return nil, ErrNoSuchKey
		}
		return nil, err
	}
	closer.Close()
	return dat, nil
}

// Set puts an entry to the DB
func (pds *PebbleDriverStorage) Set(key []byte, value []byte, options *SetOptions) error {
	if options == nil {
		options = &SetOptions{}
	}
	return pds.db.Set(key, value, &pebble.WriteOptions{
		Sync: options.Synchronized,
	})
}

// Delete deletes an entry in the DB
func (pds *PebbleDriverStorage) Delete(key []byte) error {
	return pds.db.Delete(key, nil)
}

// DeleteRange deletes a set of entries in the DB
func (pds *PebbleDriverStorage) DeleteRange(start []byte, end []byte) error {
	return pds.db.DeleteRange(start, end, nil)
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

// Delete deletes an entry in the DB
func (pdb *PebbleDriverBatch) Delete(key []byte) error {
	return pdb.batch.Delete(key, nil)
}

// DeleteRange deletes a set of entries in the DB
func (pdb *PebbleDriverBatch) DeleteRange(start []byte, end []byte) error {
	return pdb.batch.DeleteRange(start, end, nil)
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

// Key returns current key
func (pdi *PebbleDriverIterator) Key() []byte {
	return pdi.it.Key()
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
