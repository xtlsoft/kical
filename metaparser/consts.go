// Package metaparser provides utilities to manipulate the
// metadata of a kical table
package metaparser

// MetaInitCharacter as is
const MetaInitCharacter = byte('&')

// Metadata Second Character Enum
const (
	MetaTypeStorageType = byte(':')
	MetaTypeExtended    = byte('!')
	MetaTypeKeys        = byte('|')
	MetaTypeTableName   = byte('@')
	MetaTypePrimaryKey  = byte('*')
)

// MetaKeysSeparator as is
const MetaKeysSeparator = byte('|')

// Metadata Extended Enum
const (
	MetaTypeExtendedK = byte('k')
)

// Metadata Primary Key Type
const (
	MetaPrimaryKeyAutoIncrementID = byte('0')
	MetaPrimaryKeyUUID            = byte('1')
	MetaPrimaryKeyCustom          = byte('2')
)

// Metadata Storage Type
const (
	MetaStorageTypeKV          = byte('a')
	MetaStorageTypeRowDocument = byte('b')
	MetaStorageTypeColumn      = byte('c')
	MetaStorageTypeAnalytical  = byte('d')
)
