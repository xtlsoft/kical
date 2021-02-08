// Package metaparser provides utilities to manipulate the
// metadata of a kical table
package metaparser

// MetaInitCharacter as is
const MetaInitCharacter = '&'

// Metadata Second Character Enum
const (
	MetaTypeStorageType = ':'
	MetaTypeExtended    = '!'
	MetaTypeKeys        = '|'
	MetaTypeTableName   = '@'
	MetaTypePrimaryKey  = '*'
)

// MetaKeysSeparator as is
const MetaKeysSeparator = '|'

// Metadata Extended Enum
const (
	MetaTypeExtendedK = 'k'
)

// Metadata Primary Key Type
const (
	MetaPrimaryKeyAutoIncrementID = '0'
	MetaPrimaryKeyUUID            = '1'
	MetaPrimaryKeyCustom          = '2'
)

// Metadata Storage Type
const (
	MetaStorageTypeRowDocument = 'a'
	MetaStorageTypeColumn      = 'b'
	MetaStorageTypeAnalytical  = 'c'
)
