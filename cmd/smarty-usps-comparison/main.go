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

type ZipcodeResult struct {
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

func ProcessSmartyResponse(responseBody []smarty.SmartyResponse, zipToCounties map[string][]string, zipcodes []string, startIndex int) ([]ZipcodeResult, error) {
	processedResponses := make([]ZipcodeResult, 0)

	for _, response := range responseBody {
		batchAdjustedIndex := startIndex + response.InputIndex
		if batchAdjustedIndex < 0 || batchAdjustedIndex >= len(zipcodes) {
			return nil, fmt.Errorf("batch adjusted index %d is out of bounds, indicating a possible issue with API response", batchAdjustedIndex)
		}

		if response.Status != "" {
			result := ZipcodeResult{
				ErrorMessage: fmt.Sprintf("Zip code input: %s, Status response: %s, Reason: %s", zipcodes[batchAdjustedIndex], response.Status, response.Reason),
				USPSFips:     zipToCounties[zipcodes[batchAdjustedIndex]],
				SmartyFips:   []string{},
				Zipcode:      zipcodes[batchAdjustedIndex],
			}
			processedResponses = append(processedResponses, result)
			continue
		}

		for _, zipcode := range response.Zipcodes {
			result := ZipcodeResult{
				Zipcode:  zipcode.Zipcode,
				USPSFips: zipToCounties[zipcode.Zipcode],
			}

			result.SmartyFips = append(result.SmartyFips, getLastThreeChars(zipcode.CountyFIPS))

			for _, altCounty := range zipcode.AlternateCounties {
				result.SmartyFips = append(result.SmartyFips, getLastThreeChars(altCounty.CountyFIPS))
			}

			mismatches := countMismatches(result.USPSFips, result.SmartyFips)
			result.MismatchCount = mismatches
			if mismatches > 0 {
				result.ErrorMessage = fmt.Sprintf("Mismatches found: %d", mismatches)
			}

			processedResponses = append(processedResponses, result)
		}
	}

	return processedResponses, nil
}

func getLastThreeChars(s string) string {
	if len(s) >= 3 {
		return s[len(s)-3:]
	}
	return ""
}

func countMismatches(uspsFIPS, smartyFIPS []string) int {
	sort.Strings(uspsFIPS)
	sort.Strings(smartyFIPS)

	i, j := 0, 0
	mismatches := 0

	for i < len(uspsFIPS) && j < len(smartyFIPS) {
		if uspsFIPS[i] == smartyFIPS[j] {
			i++
			j++
		} else if uspsFIPS[i] < smartyFIPS[j] {
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
	mismatches += len(uspsFIPS) - i
	mismatches += len(smartyFIPS) - j

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
	authId := mustGetenv("AUTH_ID")
	authToken := mustGetenv("AUTH_TOKEN")
	client := smarty.NewSmartyClient(authId, authToken)
	uspsZipToCountyFile := flag.String("csv", "", "CSV file path containing zip to county mappings for USPS data")
	flag.Parse()

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

		processedResponses, err := ProcessSmartyResponse(responseBody, zipToCounty, zips, i)

		if err != nil {
			log.Printf("Error processing responses: %v", err)
			continue
		}

		for _, response := range processedResponses {
			record := []string{
				response.Zipcode,
				strings.Join(response.USPSFips, ","),
				strings.Join(response.SmartyFips, ","),
				strconv.Itoa(response.MismatchCount),
				response.ErrorMessage,
			}
			if err := writer.Write(record); err != nil {
				log.Printf("Error writing to CSV: %v", err)
			}
		}

		log.Printf("%v zips processed.", i+batchSize)

		time.Sleep(rateLimitPause)
	}

	fmt.Println("All zip codes processed.")
}
