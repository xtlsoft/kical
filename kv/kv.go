package kv

import (
	"bytes"
	"encoding/gob"

	"github.com/xtlsoft/kical/common"
	"github.com/xtlsoft/kical/storage"
)

// NewKV initializes a new KV document
func NewKV(conf *common.DatabaseConfigure, bucket storage.Storage) *KV {
	return &KV{
		conf:   conf,
		bucket: bucket,
	}
}

// KV is the KV table
// KV data structure acts like map[string]interface{}
type KV struct {
	conf   *common.DatabaseConfigure
	bucket storage.Storage
}

// Get gets an entry from the KV table
func (t *KV) Get(key string) (interface{}, error) {
	r, err := t.bucket.Get([]byte(string(keyInitialCharacter) + key))
	if err != nil {
		return nil, err
	}
	decoder := gob.NewDecoder(bytes.NewBuffer(r))
	var ret interface{}
	err = decoder.Decode(ret)
	if err != nil {
		return nil, err
	}
	return ret, nil
}
