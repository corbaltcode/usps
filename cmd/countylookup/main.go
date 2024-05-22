package main

import (
	"fmt"
	"os"

	"github.com/corbaltcode/usps/counties"
)

func main() {
	if len(os.Args) < 2 {
		fmt.Fprintf(os.Stderr, "Error: Missing tar file name\nUsage: %s <tar_file_name>\n", os.Args[0])
		os.Exit(1)
	}

	tarName := os.Args[1]

	zipPassword := mustGetenv("ZIP_PASSWORD")

	zipToCounty, err := counties.CollectZip4Details(tarName, zipPassword)
	if err != nil {
		fmt.Fprintf(os.Stderr, "Failed to collect details: %v\n", err)
		os.Exit(1)
	}

	for zip, counties := range zipToCounty {
		fmt.Printf("ZIP Code: %s, County Numbers: %v\n", zip, counties)
	}
}

func mustGetenv(key string) string {
	v, ok := os.LookupEnv(key)
	if !ok {
		panic(fmt.Sprintf("missing env var: %v", key))
	}
	return v
}
