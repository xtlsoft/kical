package kv_test

import (
	"testing"

	"github.com/xtlsoft/kical"
	"github.com/xtlsoft/kical/storage"
)

func BenchmarkGet(b *testing.B) {
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
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		kv.Get("a")
	}
}

func BenchmarkAnother(b *testing.B) {
	arr := []int{0, 1, 2, 3}
	for i := 4; i < 4000000; i++ {
		arr = append(arr, i)
	}
	b.ResetTimer()
	// arr = append([]int{1}, arr...)
	arr2 := make([]int, len(arr)+1)
	arr2[0] = 0
	for i := 0; i < b.N; i++ {
		copy(arr2[1:], arr[0:])
	}
}

func BenchmarkAnothe2(b *testing.B) {
	str := ""
	for i := 1; i < 100; i++ {
		str += "1"
	}
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_ = string([]byte(str))
	}
}
