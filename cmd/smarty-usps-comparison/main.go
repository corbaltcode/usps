package main

import (
	"encoding/csv"
	"flag"
	"fmt"
	"io"
	"log"
	"os"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/corbaltcode/usps/internal/smarty"
)

type ZIPCountyDiff struct {
	Zipcode       string
	USPSFips      []string
	SmartyFips    []string
	MismatchCount int
	ErrorMessage  string
}

func parseCSVList(dataString string) []string {
	items := strings.Split(dataString, ",")
	for i, item := range items {
		items[i] = strings.TrimSpace(item)
	}
	return items
}

func readZipToCountyDataFromCSV(reader io.Reader) ([]string, map[string][]string, error) {
	csvReader := csv.NewReader(reader)
	header, err := csvReader.Read()
	if err != nil {
		return nil, nil, fmt.Errorf("failed to read header: %w", err)
	}

	if len(header) < 2 || header[0] != "ZIP Code" || header[1] != "County Numbers" {
		return nil, nil, fmt.Errorf("unexpected header format")
	}

	var zips []string
	zipToCounty := make(map[string][]string)

	for {
		record, err := csvReader.Read()
		if err == io.EOF {
			break
		}
		if err != nil {
			return nil, nil, fmt.Errorf("failed to read record: %w", err)
		}

		zip := record[0]
		counties := parseCSVList(record[1])

		zips = append(zips, zip)
		zipToCounty[zip] = counties
	}

	return zips, zipToCounty, nil
}

func generateZipCountyDiff(zipcode string, smartyResponse smarty.Response, countyMapping map[string][]string) ZIPCountyDiff {
	errorMessage := ""

	if smartyResponse.Status != "" {
		errorMessage = fmt.Sprintf("ZIP code input: %s, Status response: %s, Reason: %s", zipcode, smartyResponse.Status, smartyResponse.Reason)
	}
	uspsFips := countyMapping[zipcode]
	smartyFips := extractFipsCodes(smartyResponse.Zipcodes)

	mismatches := countMismatches(uspsFips, smartyFips)

	if mismatches > 0 {
		errorMessage = fmt.Sprintf("Mismatches found: %d", mismatches)
	}

	return ZIPCountyDiff{
		Zipcode:       zipcode,
		USPSFips:      uspsFips,
		SmartyFips:    smartyFips,
		MismatchCount: mismatches,
		ErrorMessage:  errorMessage,
	}
}

func extractFipsCodes(smartyResponse []smarty.Zipcode) []string {
	fipsCodes := make([]string, 0)

	if smartyResponse == nil {
		return fipsCodes
	}

	for _, zipcode := range smartyResponse {
		fipsCodes = append(fipsCodes, getLastThreeChars(zipcode.CountyFIPS))

		for _, altCounty := range zipcode.AlternateCounties {
			fipsCodes = append(fipsCodes, getLastThreeChars(altCounty.CountyFIPS))
		}
	}

	return fipsCodes
}

func getLastThreeChars(s string) string {
	if len(s) >= 3 {
		return s[len(s)-3:]
	}
	return ""
}

func countMismatches(stringSliceA, stringSliceB []string) int {
	sort.Strings(stringSliceA)
	sort.Strings(stringSliceB)

	i, j := 0, 0
	mismatches := 0

	for i < len(stringSliceA) && j < len(stringSliceB) {
		if stringSliceA[i] == stringSliceB[j] {
			i++
			j++
		} else if stringSliceA[i] < stringSliceB[j] {
			// Usps has an element that smarty doesn't have.
			mismatches++
			i++
		} else {
			// Smarty has an element that usps doesn't have.
			mismatches++
			j++
		}
	}

	// Count any remaining elements as mismatches.
	mismatches += len(stringSliceA) - i
	mismatches += len(stringSliceB) - j

	return mismatches
}

func mustGetenv(key string) string {
	v, ok := os.LookupEnv(key)
	if !ok {
		log.Fatalf("missing required environment variable: %s", key)
	}
	return v
}

func setupCSVWriter(output io.Writer) *csv.Writer {
	writer := csv.NewWriter(output)
	headers := []string{"Zipcode", "USPS Fips", "Smarty Fips", "Mismatch Count", "Error"}
	if err := writer.Write(headers); err != nil {
		log.Fatalf("Error writing headers to CSV: %v", err)
	}
	return writer
}

func main() {
	uspsZipToCountyFile := flag.String("csv", "", "CSV file path containing zip to county mappings for USPS data")
	flag.Parse()
	authId := mustGetenv("AUTH_ID")
	authToken := mustGetenv("AUTH_TOKEN")
	client := smarty.NewClient(authId, authToken)

	var reader io.Reader

	if *uspsZipToCountyFile == "" {
		reader = os.Stdin
	} else {
		file, err := os.Open(*uspsZipToCountyFile)
		if err != nil {
			log.Fatalf("Error opening file: %v", err)
		}
		defer file.Close()
		reader = file
	}

	zips, zipToCounty, err := readZipToCountyDataFromCSV(reader)
	if err != nil {
		log.Fatalf("Error reading CSV file: %v", err)
	}

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

		if len(responseBody) > len(batch) {
			log.Printf("Received more responses (%d) than the number of queried ZIPs (%d)", len(responseBody), len(batch))
			continue
		}

		for j, response := range responseBody {
			zip := batch[j]
			diff := generateZipCountyDiff(zip, response, zipToCounty)

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
