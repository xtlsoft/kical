package kical

import (
	"github.com/xtlsoft/kical/common"
	"github.com/xtlsoft/kical/kv"
	"github.com/xtlsoft/kical/metaparser"
	"github.com/xtlsoft/kical/storage"
)

// NewDatabase creates a new database instance
func NewDatabase(driver storage.Driver, conf *common.DatabaseConfigure) (*Database, error) {
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

// Database is the main interface class of Kical
type Database struct {
	driver storage.Driver
	conf   *common.DatabaseConfigure
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
	typ        byte
	KV         *kv.KV
}

func (tbl *Table) init() error {
	tbl.metaParser = metaparser.NewParser(tbl.bucket)
	typ, err := tbl.metaParser.GetStorageType()
	if err == storage.ErrNoSuchKey {
		// TODO: ask user to create a table
		return err
	}
	tbl.typ = typ
	switch typ {
	case metaparser.MetaStorageTypeKV:
		tbl.KV = kv.NewKV(tbl.db.conf, tbl.bucket)
	// TODO: complete this
	default:
		panic("Reaching theoretical unreachable code")
	}
	return nil
}

// GetDatabase returns tbl.db
func (tbl *Table) GetDatabase() *Database {
	return tbl.db
}

// GetStorage returns tbl.bucket
func (tbl *Table) GetStorage() storage.Storage {
	return tbl.bucket
}

// IsKV as is
func (tbl *Table) IsKV() bool {
	return tbl.typ == metaparser.MetaStorageTypeKV
}

// IsRowDocument as is
func (tbl *Table) IsRowDocument() bool {
	return tbl.typ == metaparser.MetaStorageTypeRowDocument
}

// IsColumn as is
func (tbl *Table) IsColumn() bool {
	return tbl.typ == metaparser.MetaStorageTypeColumn
}

// IsAnalytical as is
func (tbl *Table) IsAnalytical() bool {
	return tbl.typ == metaparser.MetaStorageTypeAnalytical
}

// GetKV returns tbl.KV or returns common.ErrWrongStorageType
func (tbl *Table) GetKV() (*kv.KV, error) {
	if !tbl.IsKV() {
		return nil, common.ErrWrongStorageType
	}
	return tbl.KV, nil
}
