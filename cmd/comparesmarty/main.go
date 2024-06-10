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

	"github.com/corbaltcode/usps/internal/processsmarty"
)

type ZipcodeRequest struct {
	Zipcode string `json:"zipcode"`
}

func readCSV(fileName string) ([]string, map[string][]string, error) {
	file, err := os.Open(fileName)
	if err != nil {
		return nil, nil, fmt.Errorf("failed to open file: %w", err)
	}
	defer file.Close()

	reader := csv.NewReader(file)
	header, err := reader.Read()
	if err != nil {
		return nil, nil, fmt.Errorf("failed to read header: %w", err)
	}

	if len(header) < 2 || header[0] != "ZIP Code" || header[1] != "County Numbers" {
		return nil, nil, fmt.Errorf("unexpected header format")
	}

	var zips []string
	zipToCounties := make(map[string][]string)

	for {
		record, err := reader.Read()
		if err == io.EOF {
			break
		}
		if err != nil {
			return nil, nil, fmt.Errorf("failed to read record: %w", err)
		}

		zip := record[0]
		counties := parseCounties(record[1])

		zips = append(zips, zip)
		zipToCounties[zip] = counties
	}

	return zips, zipToCounties, nil
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
	uspsCountyFile := flag.String("source", "", "Source file path for the CSV data")
	flag.Parse()

	if *uspsCountyFile == "" {
		log.Fatal("You must specify a source file path using the --source flag.")
	}

	zips, zipToCounties, err := readCSV(*uspsCountyFile)
	if err != nil {
		log.Fatalf("Error reading CSV file: %v", err)
	}

	comparisonResults := make([]processsmarty.ZipcodeResult, 0)

	yield := func(result processsmarty.ZipcodeResult) {
		comparisonResults = append(comparisonResults, result)

		if result.Inconsistencies > 0 {
			log.Printf("%+v", result)
		}

	}

	const batchSize = 100
	const rateLimitPause = 3 * time.Second

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

		results := processsmarty.ProcessSmartyResponse(responseBody, zipToCounties, yield)
		fmt.Println(results)

		log.Printf("Pausing for %v to handle rate limiting", rateLimitPause)
		log.Printf("%v zips processesd", i)
		time.Sleep(rateLimitPause)
	}

	fmt.Println("All ZIP codes processed.")
}
