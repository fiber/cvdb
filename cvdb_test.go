package cvdb

import (
	"bytes"
	"fmt"
	"strconv"
	"strings"
	"testing"
	"time"
)

const (
	dbname  = "/var/tmp/__cvdb_test.db"
	numkeys = 1000000
)

var opts = Options{
	Offset: 1024,
}

func TestWrite001(t *testing.T) {
	tstart := time.Now()
	db, err := CreateOpts(dbname, &opts)
	//db.dumpDistrib = true
	if err != nil {
		t.Fatalf("error in create: %v", err)
	}
	for i := 0; i < numkeys; i++ {
		ks := "test-" + strconv.FormatInt(int64(i), 36) + "-test"
		db.Put([]byte(ks), []byte(ks))
	}
	if err := db.Commit(); err != nil {
		t.Fatalf("error in commit: %v", err)
	}
	db.Close()
	dura := time.Since(tstart)
	fmt.Printf("wrote %v keys in %v (%v/key), %v keyskips\n", numkeys, dura, dura/numkeys, db.skipped)
}

func TestRead001(t *testing.T) {
	tstart := time.Now()
	var tfirst time.Duration
	db, err := OpenOpts(dbname, &opts)
	if err != nil {
		t.Fatalf("error in open: %v", err)
	}
	buffer := make([]byte, 2028)
	for i := 0; i < numkeys; i++ {
		ks := "test-" + strconv.FormatInt(int64(i), 36) + "-test"
		val, err := db.GetBuffer([]byte(ks), buffer)
		if err != nil {
			t.Fatalf("read error %v", err)
		}
		if val == nil {
			t.Fatalf("could not read key %v", ks)
		}
		if !bytes.Equal([]byte(ks), val) {
			t.Fatalf("key %v returned invalid value", ks)
		}
		if i == 0 {
			tfirst = time.Since(tstart)
		}
	}
	dura := time.Since(tstart)
	fmt.Printf("read  %v keys in %v (%v/key) (time to first read %v)\n", numkeys, dura, dura/numkeys, tfirst)
	db.Close()
}

func TestIterate001(t *testing.T) {
	tstart := time.Now()
	db, err := OpenOpts(dbname, &opts)
	if err != nil {
		t.Fatalf("error in open: %v", err)
	}
	iter := db.Iterator()
	cnt := 0
	for k, v := iter.First(); k != nil; k, v = iter.Next() {
		ks := string(k)
		if !strings.HasPrefix(ks, "test-") && !strings.HasSuffix(ks, "-test") {
			t.Fatalf("invalid key %v (len %v) - value %v", ks, len(k), string(v))
		}
		vs := string(v)
		if vs != ks {
			t.Fatalf("invalid value for key %v", ks)
		}
		cnt++
	}
	if err := iter.Err(); err != nil {
		t.Fatalf("iterator error %v", err)
	}
	if cnt != numkeys {
		t.Errorf("iterator returned %v keys (%v) expected", cnt, numkeys)
	}
	dura := time.Since(tstart)
	fmt.Printf("iter  %v keys in %v (%v/key)\n", numkeys, dura, dura/numkeys)
	db.Close()
}

func TestIterateKeysOnly001(t *testing.T) {
	tstart := time.Now()
	db, err := OpenOpts(dbname, &opts)
	if err != nil {
		t.Fatalf("error in open: %v", err)
	}
	iter := db.Iterator()
	iter.KeysOnly = true
	cnt := 0
	for k, _ := iter.First(); k != nil; k, _ = iter.Next() {
		ks := string(k)
		if !strings.HasPrefix(ks, "test-") && !strings.HasSuffix(ks, "-test") {
			t.Fatalf("invalid key %v (len %v)", ks, len(k))
		}
		cnt++
	}
	if err := iter.Err(); err != nil {
		t.Fatalf("iterator error %v", err)
	}
	if cnt != numkeys {
		t.Errorf("iterator returned %v keys (%v) expected", cnt, numkeys)
	}
	dura := time.Since(tstart)
	fmt.Printf("keysonly  %v keys in %v (%v/key)\n", numkeys, dura, dura/numkeys)
	db.Close()
}

var sV = sv()

func sv() []byte {
	var s []byte
	for i := 0; i < 100000; i++ {
		b := []byte(strconv.FormatInt(int64(i), 36))
		s = append(s, b...)
	}
	return s
}

func BenchmarkFNV1a(b *testing.B) {
	for i := 0; i < b.N; i++ {
		for j := 0; j < 1000; j++ {
			_ = FNV1a.Hash(sV[j*10 : j*10+j])
		}
	}
}

func BenchmarkCDB2a(b *testing.B) {
	for i := 0; i < b.N; i++ {
		for j := 0; j < 1000; j++ {
			_ = CDB2a.Hash(sV[j*10 : j*10+j])
		}
	}
}

func BenchmarkMurmur3(b *testing.B) {
	for i := 0; i < b.N; i++ {
		for j := 0; j < 1000; j++ {
			_ = Murmur3.Hash(sV[j*10 : j*10+j])
		}
	}
}
