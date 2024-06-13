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
	"sort"
	"strings"
	"time"
)

type SmartyResponse struct {
	InputIndex int       `json:"input_index"`
	Status     string    `json:"status,omitempty"`
	Reason     string    `json:"reason,omitempty"`
	Zipcodes   []Zipcode `json:"zipcodes,omitempty"`
}

type Zipcode struct {
	Zipcode           string            `json:"zipcode"`
	ZipcodeType       string            `json:"zipcode_type"`
	DefaultCity       string            `json:"default_city"`
	CountyFIPS        string            `json:"county_fips"`
	CountyName        string            `json:"county_name"`
	StateAbbreviation string            `json:"state_abbreviation"`
	State             string            `json:"state"`
	Latitude          float64           `json:"latitude"`
	Longitude         float64           `json:"longitude"`
	Precision         string            `json:"precision"`
	AlternateCounties []AlternateCounty `json:"alternate_counties"`
}

type AlternateCounty struct {
	CountyFIPS        string `json:"county_fips"`
	CountyName        string `json:"county_name"`
	StateAbbreviation string `json:"state_abbreviation"`
	State             string `json:"state"`
}

type ZipcodeResult struct {
	Zipcode      string
	USPSFips     []string
	SmartyFips   []string
	ErrorMessage string
}

type ZipcodeRequest struct {
	Zipcode string `json:"zipcode"`
}

type SmartyClient struct {
	AuthId    string
	AuthToken string
	BaseURL   string
}

func NewSmartyClient(authId, authToken string) *SmartyClient {
	return &SmartyClient{
		AuthId:    authId,
		AuthToken: authToken,
		BaseURL:   "https://us-zipcode.api.smarty.com/lookup",
	}
}

func (client *SmartyClient) QueryBatch(zips []string) ([]SmartyResponse, error) {
	var payload []ZipcodeRequest
	for _, zip := range zips {
		payload = append(payload, ZipcodeRequest{Zipcode: zip})
	}
	jsonPayload, err := json.Marshal(payload)
	if err != nil {
		return nil, fmt.Errorf("failed to marshal JSON payload: %w", err)
	}

	apiURL := fmt.Sprintf("%s?auth-id=%s&auth-token=%s", client.BaseURL, client.AuthId, client.AuthToken)

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

	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("API request failed with status %d: %s", resp.StatusCode, body)
	}

	var smartyResponses []SmartyResponse
	err = json.Unmarshal(body, &smartyResponses)
	if err != nil {
		return nil, fmt.Errorf("failed to unmarshal response: %w", err)
	}

	return smartyResponses, nil
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

func parseCSVList(dataString string) []string {
	items := strings.Split(dataString, ",")
	for i, item := range items {
		items[i] = strings.TrimSpace(item)
	}
	return items
}

func ProcessSmartyResponse(responseBody []SmartyResponse, zipToCounties map[string][]string, zipCodes []string) ([]ZipcodeResult, error) {
	processedResponses := make([]ZipcodeResult, 0)

	for _, response := range responseBody {
		if response.Status != "" {
			result := ZipcodeResult{
				ErrorMessage: fmt.Sprintf("Invalid zip code: %s - %s, Reason: %s", zipCodes[response.InputIndex], response.Status, response.Reason),
				USPSFips:     zipToCounties[zipCodes[response.InputIndex]],
				SmartyFips:   []string{},
				Zipcode:      zipCodes[response.InputIndex],
			}
			processedResponses = append(processedResponses, result)
			continue
		}

		for _, zipcode := range response.Zipcodes {
			result := ZipcodeResult{
				Zipcode:  zipcode.Zipcode,
				USPSFips: zipToCounties[zipcode.Zipcode],
			}

			if len(zipcode.CountyFIPS) >= 3 {
				result.SmartyFips = append(result.SmartyFips, zipcode.CountyFIPS[len(zipcode.CountyFIPS)-3:])
			}

			for _, altCounty := range zipcode.AlternateCounties {
				if len(altCounty.CountyFIPS) >= 3 {
					result.SmartyFips = append(result.SmartyFips, altCounty.CountyFIPS[len(altCounty.CountyFIPS)-3:])
				}
			}

			mismatches := countMismatches(result.USPSFips, result.SmartyFips)
			if mismatches > 0 {
				result.ErrorMessage = fmt.Sprintf("Mismatches found: %d", mismatches)
			}

			processedResponses = append(processedResponses, result)
		}
	}

	return processedResponses, nil
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
			// Usps has an element that smarty doesn't have
			mismatches++
			i++
		} else {
			// Smarty has an element that usps doesn't have
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

func main() {
	authId := mustGetenv("AUTH_ID")
	authToken := mustGetenv("AUTH_TOKEN")
	client := NewSmartyClient(authId, authToken)
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

	const batchSize = 100
	const rateLimitPause = 2 * time.Second

	for i := 0; i < len(zips); i += batchSize {
		end := i + batchSize
		if end > len(zips) {
			end = len(zips)
		}
		batch := zips[i:end]
		responseBody, err := client.QueryBatch(batch)

		if err != nil {
			log.Printf("Error querying Smarty API: %v", err)
		}

		processedResponses, err := ProcessSmartyResponse(responseBody, zipToCounty, zips)

		if err != nil {
			log.Printf("Error processing responses: %v", err)
		}

		for _, response := range processedResponses {
			if response.ErrorMessage != "" {
				log.Printf("Processed Response - Zipcode: %s, USPS Fips: [%s], Smarty Fips: [%s], Error: %s",
					response.Zipcode,
					joinStrings(response.USPSFips, ","),
					joinStrings(response.SmartyFips, ","),
					response.ErrorMessage)
			}
		}

		log.Printf("%v zips processed.", i+batchSize)

		time.Sleep(rateLimitPause)
	}

	fmt.Println("All zip codes processed.")
}

func joinStrings(items []string, sep string) string {
	if len(items) == 0 {
		return ""
	}
	result := items[0]
	for _, item := range items[1:] {
		result += sep + item
	}
	return result
}
