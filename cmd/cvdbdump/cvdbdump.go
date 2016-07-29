package main

import (
	"flag"
	"fmt"
	"os"

	"github.com/fiber/cvdb"
)

func main() {
	offset := flag.Int64("offset", 0, "offset to use")
	keysOnly := flag.Bool("keysonly", false, "keys only")
	noVal := flag.Bool("noval", false, "dont print values")
	flag.Parse()
	for _, fn := range flag.Args() {
		opts := cvdb.Options{Offset: *offset}
		db, err := cvdb.OpenOpts(fn, &opts)
		if err != nil {
			fmt.Fprintf(os.Stderr, "failed to read %v: %v", fn, err)
			os.Exit(1)
		}
		iter := db.Iterator()
		iter.KeysOnly = *keysOnly
		for iter.Next() {
			if *keysOnly {
				fmt.Printf("%v\n", string(iter.Key()))
			} else {
				k := iter.Key()
				v := iter.Value()
				ks := string(k)
				kv := string(v)
				if *noVal {
					fmt.Printf("+%v,%v:%v->...(%v bytes)\n", len(k), len(v), ks, len(v))
				} else {
					fmt.Printf("+%v,%v:%v->%v\n", len(k), len(v), ks, kv)
				}
			}
		}
		if err := iter.Err(); err != nil {
			fmt.Fprintf(os.Stderr, "dump error %v\n", err)
			os.Exit(1)
		}
		db.Close()
	}
}
