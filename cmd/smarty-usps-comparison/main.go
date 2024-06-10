package main

import (
	"bytes"
	"encoding/csv"
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"log"
	"net/http"
	"os"
	"strings"
	"time"

	"github.com/corbaltcode/usps/internal/smartyresponseprocessing"
)

type ZipcodeRequest struct {
	Zipcode string `json:"zipcode"`
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
		counties := parseCounties(record[1])

		zips = append(zips, zip)
		zipToCounty[zip] = counties
	}

	return zips, zipToCounty, nil
}

func parseCounties(countyData string) []string {
	counties := strings.Split(countyData, ",")
	for i, county := range counties {
		counties[i] = strings.TrimSpace(county)
	}
	return counties
}

func querySmartyAPIBatch(zips []string, authId string, authToken string) ([]byte, error) {
	apiURL := fmt.Sprintf("https://us-zipcode.api.smarty.com/lookup?auth-id=%s&auth-token=%s", authId, authToken)

	var payload []ZipcodeRequest
	for _, zip := range zips {
		payload = append(payload, ZipcodeRequest{Zipcode: zip})
	}
	jsonPayload, err := json.Marshal(payload)
	if err != nil {
		return nil, fmt.Errorf("failed to marshal JSON payload: %w", err)
	}

	req, err := http.NewRequest("POST", apiURL, bytes.NewBuffer(jsonPayload))
	if err != nil {
		return nil, fmt.Errorf("failed to create HTTP request: %w", err)
	}
	req.Header.Set("Content-Type", "application/json; charset=utf-8")

	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return nil, fmt.Errorf("failed to query Smarty API: %w", err)
	}
	defer resp.Body.Close()

	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, fmt.Errorf("failed to read response body: %w", err)
	}

	if resp.StatusCode == http.StatusTooManyRequests {
		return nil, fmt.Errorf("rate limit exceeded: %s", body)
	}

	return body, nil
}

func mustGetenv(key string) string {
	v, ok := os.LookupEnv(key)
	if !ok {
		log.Fatalf("missing required environment variable: %s", key)
	}
	return v
}

func main() {
	authId := mustGetenv("AUTH_ID")
	authToken := mustGetenv("AUTH_TOKEN")
	uspsZipToCountyFile := flag.String("csv", "", "CSV file path containing zip to county mappings for USPS data")
	flag.Parse()

	var reader io.Reader

	// We can read from standard input or from a specified csv file.
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

	yield := func(result smartyresponseprocessing.ZipcodeResult) {
		if result.Inconsistencies > 0 {
			log.Printf("%+v", result)
		}

	}

	const batchSize = 100
	const rateLimitPause = 2 * time.Second

	for i := 0; i < len(zips); i += batchSize {
		end := i + batchSize
		if end > len(zips) {
			end = len(zips)
		}
		batch := zips[i:end]
		responseBody, err := querySmartyAPIBatch(batch, authId, authToken)

		if err != nil {
			log.Printf("Error querying Smarty API: %v", err)
		}

		smartyresponseprocessing.ProcessSmartyResponse(responseBody, zipToCounty, yield)

		log.Printf("%v zips processed.", i+batchSize)

		time.Sleep(rateLimitPause)
	}

	fmt.Println("All zip codes processed.")
}
