package main

import (
	"encoding/csv"
	"flag"
	"fmt"
	"os"

	"github.com/corbaltcode/usps/internal/counties"
)

func main() {
	tarName := flag.String("tar", "", "Name of the tar file")
	zipCode := flag.String("zip", "", "Single zip to look up (optional)")
	flag.Parse()

	if *tarName == "" {
		fmt.Fprintf(os.Stderr, "Error: Missing tar file name\nUsage: %s -tar <tar_file_name> [-zip <optional_single_zip_code>]\n", os.Args[0])
		os.Exit(1)
	}

	zipPassword := mustGetenv("ZIP_PASSWORD")

	zipToCounty, err := counties.CollectZip4Details(*tarName, zipPassword)
	if err != nil {
		fmt.Fprintf(os.Stderr, "Failed to collect details: %v\n", err)
		os.Exit(1)
	}

	// Early return for the user-specified, single zip case.
	if *zipCode != "" {
		printCountiesForZIP(zipToCounty, *zipCode)
		return
	}

	err = exportToCSV(zipToCounty)
	if err != nil {
		fmt.Fprintf(os.Stderr, "Failed to export to CSV: %v\n", err)
		os.Exit(1)
	}
}

func exportToCSV(zipToCounty map[string][]string) error {
	writer := os.Stdout
	csvWriter := csv.NewWriter(writer)
	defer csvWriter.Flush()

	header := []string{"ZIP Code", "County Numbers"}
	if err := csvWriter.Write(header); err != nil {
		return fmt.Errorf("failed to write header: %w", err)
	}

	for zip, counties := range zipToCounty {
		record := []string{zip, fmt.Sprintf("%v", counties)}
		if err := csvWriter.Write(record); err != nil {
			return fmt.Errorf("failed to write record: %w", err)
		}
	}

	return nil
}

func printCountiesForZIP(zipToCounty map[string][]string, zip string) {
	if counties, found := zipToCounty[zip]; found {
		fmt.Printf("ZIP Code: %s, Counties: %v\n", zip, counties)
	} else {
		fmt.Printf("No counties found for ZIP Code: %s\n", zip)
	}
}

func mustGetenv(key string) string {
	v, ok := os.LookupEnv(key)
	if !ok {
		panic(fmt.Sprintf("missing env var: %v", key))
	}
	return v
}
