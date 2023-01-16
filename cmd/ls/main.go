package main

import (
	"fmt"
	"os"

	"github.com/corbaltcode/usps/epf"
	"github.com/dustin/go-humanize"
)

func main() {
	sess, err := epf.Login(mustGetenv("EPF_EMAIL"), mustGetenv("EPF_PASSWORD"))
	if err != nil {
		panic(err)
	}

	fs, err := sess.Files()
	if err != nil {
		panic(err)
	}

	for _, f := range fs {
		fmt.Printf(
			"%v\t%v\t%v\t%v\t%v\t%v\t%v\t%v\n",
			f.ID,
			f.Status,
			humanize.Bytes(f.Size),
			f.FulfillmentDate,
			f.Filename,
			f.Path,
			f.ProductCode,
			f.ProductID,
		)
	}
}

func mustGetenv(key string) string {
	v, ok := os.LookupEnv(key)
	if !ok {
		panic(fmt.Sprintf("missing env var: %v", key))
	}
	return v
}
