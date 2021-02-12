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
		// TODO: Determine sync option from user input configuration
		sync: false,
	}
}

func prepareKey(key string) []byte {
	return append(keyInitialCharacterBytes, []byte(key)...)
}

func unprepareKey(prepared []byte) (string, bool) {
	if prepared[0] != keyInitialCharacter {
		return "", false
	}
	return string(prepared[1:]), true
}

// KV is the KV table
// KV data structure acts like map[string]interface{}
type KV struct {
	conf   *common.DatabaseConfigure
	bucket storage.Storage
	sync   bool
}

// Get gets an entry from the KV table
func (t *KV) Get(key string) (interface{}, error) {
	r, err := t.bucket.Get(prepareKey(key))
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

// Set sets something to the kv table
func (t *KV) Set(key string, value interface{}) error {
	buf := bytes.NewBuffer([]byte{})
	encoder := gob.NewEncoder(buf)
	encoder.Encode(value)
	err := t.bucket.Set(prepareKey(key), buf.Bytes(), &storage.SetOptions{
		Synchronized: t.sync,
	})
	return err
}

// Delete deletes something in the tv table
func (t *KV) Delete(key string) error {
	return t.bucket.Delete(prepareKey(key))
}

// GetKeyList returns a full list of keys
func (t *KV) GetKeyList() ([]string, error) {
	iter := t.bucket.NewIter(nil, nil)
	iter.First()
	var ret []string
	for iter.Valid() {
		k, ok := unprepareKey(iter.Key())
		if ok {
			ret = append(ret, k)
		}
		iter.Next()
	}
	return ret, nil
}
