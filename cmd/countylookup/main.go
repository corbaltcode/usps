package main

import (
	"fmt"
	"log"
	"os"

	"github.com/corbaltcode/usps/counties"
)

func main() {
	if len(os.Args) < 2 {
		log.Fatalf("Usage: %s <tar_file_name>", os.Args[0])
	}

	tarName := os.Args[1]

	zipPassword := mustGetenv("ZIP_PASSWORD")

	zipToCounty, err := counties.CollectDetails(tarName, zipPassword)
	if err != nil {
		log.Fatalf("Failed to collect details: %v", err)
	}

	// Todo: output to CSV file. For now, print to console.
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
