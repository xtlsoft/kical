package kical

import "github.com/xtlsoft/kical/common"

// NewDatabaseConfigure creates a new database configure
func NewDatabaseConfigure() *common.DatabaseConfigure {
	return new(common.DatabaseConfigure)
}
