package kv_test

import (
	"testing"

	"github.com/xtlsoft/kical"
	"github.com/xtlsoft/kical/storage"
)

func BenchmarkGet(b *testing.B) {
	b.ResetTimer()
	drv := storage.NewPebbleDriver(&storage.PebbleDriverConfigure{
		BaseDirectory: "",
		UseMemory:     true,
	})
	db, _ := kical.NewDatabase(drv, nil)
	bkt, _ := drv.Bucket("test")
	bkt.Set([]byte("&:"), []byte("a"), nil)
	bkt.Set([]byte("=a"), []byte(""), nil)
	tbl, _ := db.Table("test")
	kv, _ := tbl.GetKV()
	_ = kv
	for i := 0; i < b.N; i++ {
		kv.Get("a")
	}
}
