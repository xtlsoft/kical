package metaparser

import (
	"github.com/xtlsoft/kical/storage"
)

// Parser is the meta parser class
type Parser struct {
	storage storage.Storage
}

// NewParser initializes a new meta parser
func NewParser(storage storage.Storage) *Parser {
	return &Parser{
		storage: storage,
	}
}

// GetStorageType returns the storage type of the table
func (p *Parser) GetStorageType() (byte, error) {
	key := []byte{MetaInitCharacter, MetaTypeStorageType}
	rs, err := p.storage.Get(key)
	if err != nil {
		return ' ', err
	}
	if len(rs) != 1 {
		return ' ', err
	}
	r := rs[0]
	if (r != MetaStorageTypeKV) &&
		(r != MetaStorageTypeRowDocument) &&
		(r != MetaStorageTypeColumn) &&
		(r != MetaStorageTypeAnalytical) {
		return ' ', ErrNoSuchStorageType
	}
	return r, nil
}
