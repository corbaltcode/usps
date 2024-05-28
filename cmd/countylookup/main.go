package main

import (
	"encoding/csv"
	"fmt"
	"os"
	"time"

	"github.com/corbaltcode/usps/internal/counties"
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

	currentDate := time.Now().Format("2006-01-02")
	fileName := fmt.Sprintf("usps_zip_county_details_%s.csv", currentDate)

	err = exportToCSV(zipToCounty, fileName)
	if err != nil {
		fmt.Fprintf(os.Stderr, "Failed to export to CSV: %v\n", err)
		os.Exit(1)
	}
}

func exportToCSV(zipToCounty map[string][]string, fileName string) error {
	file, err := os.Create(fileName)
	if err != nil {
		return fmt.Errorf("failed to create file: %w", err)
	}
	defer file.Close()

	writer := csv.NewWriter(file)
	defer writer.Flush()

	header := []string{"ZIP Code", "County Numbers"}
	if err := writer.Write(header); err != nil {
		return fmt.Errorf("failed to write header: %w", err)
	}

	for zip, counties := range zipToCounty {
		record := []string{zip, fmt.Sprintf("%v", counties)}
		if err := writer.Write(record); err != nil {
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
