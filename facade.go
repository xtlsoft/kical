package kical

import (
	"github.com/xtlsoft/kical/metaparser"
	"github.com/xtlsoft/kical/storage"
)

// NewDatabase creates a new database instance
func NewDatabase(driver storage.Driver, conf *DatabaseConfigure) (*Database, error) {
	db := &Database{
		driver: driver,
		conf:   conf,
	}
	err := db.init()
	if err != nil {
		return nil, err
	}
	return db, nil
}

// DatabaseConfigure describes the main configuration
// for kical databases when creating and manipulating
// a kical db
type DatabaseConfigure struct {
}

// Database is the main interface class of Kical
type Database struct {
	driver storage.Driver
	conf   *DatabaseConfigure
}

func (db *Database) init() error {
	return nil
}

// Table creates a table instance
func (db *Database) Table(name string) (*Table, error) {
	// TODO: finish this
	s, err := db.driver.Bucket(name)
	if err != nil {
		return nil, err
	}
	tbl := &Table{
		bucket: s,
		db:     db,
	}
	err = tbl.init()
	if err != nil {
		return nil, err
	}
	return tbl, nil
}

// Table is the basic collection type in Kical
type Table struct {
	bucket     storage.Storage
	db         *Database
	metaParser *metaparser.Parser
}

func (tbl *Table) init() error {
	tbl.metaParser = metaparser.NewParser(tbl.bucket)
	typ, err := tbl.metaParser.GetStorageType()
	if err == storage.ErrNoSuchKey {
		// TODO: ask user to create a table
	}
	switch typ {
	case metaparser.MetaStorageTypeKV:

	default:
		panic("Reaching theoretical unreachable code")
	}
	_ = typ
	return nil
}
