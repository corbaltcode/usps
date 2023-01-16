package main

import (
	"fmt"
	"io"
	"os"
	"path/filepath"

	"github.com/corbaltcode/usps/epf"
)

func main() {
	if len(os.Args) < 2 {
		fmt.Fprintf(os.Stderr, "usage: %v <file-id>\n", filepath.Base(os.Args[0]))
		os.Exit(1)
	}

	sess, err := epf.Login(mustGetenv("EPF_EMAIL"), mustGetenv("EPF_PASSWORD"))
	if err != nil {
		panic(err)
	}

	r, err := sess.Download(os.Args[1])
	if err != nil {
		panic(err)
	}

	defer r.Close()
	io.Copy(os.Stdout, r)
}

func mustGetenv(key string) string {
	v, ok := os.LookupEnv(key)
	if !ok {
		panic(fmt.Sprintf("missing env var: %v", key))
	}
	return v
}
