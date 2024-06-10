package main

import (
	"encoding/csv"
	"flag"
	"fmt"
	"os"
	"strings"

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

	// Early return for the user-specified, single ZIP case.
	if *zipCode != "" {
		if counties, found := zipToCounty[*zipCode]; found {
			fmt.Printf("ZIP Code: %s, Counties: %v\n", *zipCode, counties)
			return
		}

		fmt.Printf("No counties found for ZIP Code: %s\n", *zipCode)
		os.Exit(1)
	}

	err = exportToCSV(zipToCounty)
	if err != nil {
		fmt.Fprintf(os.Stderr, "Failed to export to CSV: %v\n", err)
		os.Exit(1)
	}
}

func exportToCSV(zipToCounty map[string][]string) error {
	csvWriter := csv.NewWriter(os.Stdout)

	defer csvWriter.Flush()

	header := []string{"ZIP Code", "County Numbers"}
	if err := csvWriter.Write(header); err != nil {
		return fmt.Errorf("failed to write header: %w", err)
	}

	for zip, counties := range zipToCounty {
		joinedCounties := strings.Join(counties, ",")
		record := []string{zip, joinedCounties}
		if err := csvWriter.Write(record); err != nil {
			return fmt.Errorf("failed to write record: %w", err)
		}
	}

	return nil
}

func mustGetenv(key string) string {
	v, ok := os.LookupEnv(key)
	if !ok {
		panic(fmt.Sprintf("missing env var: %v", key))
	}
	return v
}
