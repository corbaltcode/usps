package main

import (
	"encoding/csv"
	"flag"
	"fmt"
	"io"
	"log"
	"os"
	"strconv"
	"strings"
	"time"

	"github.com/corbaltcode/usps/internal/smarty"
	"github.com/corbaltcode/usps/internal/ziptocounty"
)

func main() {
	tarName := flag.String("tar", "", "Name of the tar file")

	flag.Parse()

	authId := mustGetenv("AUTH_ID")
	authToken := mustGetenv("AUTH_TOKEN")
	client := smarty.NewClient(authId, authToken)

	if *tarName == "" {
		fmt.Fprintf(os.Stderr, "Error: Missing tar file name\nUsage: %s -tar <tar_file_name> \n", os.Args[0])
		os.Exit(1)
	}

	zipPassword := mustGetenv("ZIP_PASSWORD")
	log.Printf("Extracting USPS zip data from %v and mapping zips to corresponding USPS counties...\n", *tarName)

	zipToCounty, err := ziptocounty.CollectUSPSZip4Details(*tarName, zipPassword)
	if err != nil {
		fmt.Fprintf(os.Stderr, "Failed to collect details: %v\n", err)
		os.Exit(1)
	}

	// Get the list of zip codes for batch requests.
	zips := getZipCodes(zipToCounty)

	writer := setupCSVWriter(os.Stdout)
	defer writer.Flush()

	const batchSize = 100
	const rateLimitPause = 2 * time.Second

	for i := 0; i < len(zips); i += batchSize {
		end := min(i+batchSize, len(zips))
		batch := zips[i:end]
		responseBody, err := client.QueryBatch(batch)

		if err != nil {
			log.Printf("Error querying Smarty API: %v", err)
			continue
		}

		if len(responseBody) != len(batch) {
			log.Printf("Mismatched response count: received %d responses for %d queried ZIPs", len(responseBody), len(batch))
			continue
		}

		for j, response := range responseBody {
			zip := batch[j]
			diff := ziptocounty.GenerateSmartyUSPSDiff(zip, response, zipToCounty)

			record := []string{
				diff.Zipcode,
				strings.Join(diff.USPSFips, ","),
				strings.Join(diff.SmartyFips, ","),
				strconv.Itoa(diff.MismatchCount),
				diff.ErrorMessage,
			}
			if err := writer.Write(record); err != nil {
				log.Printf("Error writing to CSV: %v", err)
			}
		}

		log.Printf("%v zips processed.", i+batchSize)
		time.Sleep(rateLimitPause)
	}

}

func getZipCodes(zipToCounty map[string][]string) []string {
	zipCodes := make([]string, 0, len(zipToCounty))
	for zip := range zipToCounty {
		zipCodes = append(zipCodes, zip)
	}
	return zipCodes
}

func setupCSVWriter(output io.Writer) *csv.Writer {
	writer := csv.NewWriter(output)
	headers := []string{"Zipcode", "USPS Fips", "Smarty Fips", "Mismatch Count", "Error"}
	if err := writer.Write(headers); err != nil {
		log.Fatalf("Error writing headers to CSV: %v", err)
	}
	return writer
}

func mustGetenv(key string) string {
	v, ok := os.LookupEnv(key)
	if !ok {
		panic(fmt.Sprintf("missing env var: %v", key))
	}
	return v
}
