package smarty

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/url"
)

type zipcodeRequest struct {
	Zipcode string `json:"zipcode"`
}

type Response struct {
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
	AlternateCounties []alternateCounty `json:"alternate_counties"`
}

type alternateCounty struct {
	CountyFIPS        string `json:"county_fips"`
	CountyName        string `json:"county_name"`
	StateAbbreviation string `json:"state_abbreviation"`
	State             string `json:"state"`
}

type Client struct {
	authId    string
	authToken string
	baseURL   string
}

func NewClient(authId, authToken string) *Client {
	return &Client{
		authId:    authId,
		authToken: authToken,
		baseURL:   "https://us-zipcode.api.smarty.com/lookup",
	}
}

func (client *Client) QueryBatch(zips []string) ([]Response, error) {
	var payload []zipcodeRequest
	for _, zip := range zips {
		payload = append(payload, zipcodeRequest{Zipcode: zip})
	}
	jsonPayload, err := json.Marshal(payload)
	if err != nil {
		return nil, fmt.Errorf("failed to marshal JSON payload: %w", err)
	}

	v := url.Values{}
	v.Add("auth-id", client.authId)
	v.Add("auth-token", client.authToken)
	apiURL := client.baseURL + "?" + v.Encode()

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

	var smartyResponses []Response
	err = json.Unmarshal(body, &smartyResponses)
	if err != nil {
		return nil, fmt.Errorf("failed to unmarshal response: %w", err)
	}

	return smartyResponses, nil
}

func ExtractCountyFipsCodes(zipcodes []Zipcode) []string {
	countyFipsCodes := make([]string, 0)

	if zipcodes == nil {
		return countyFipsCodes
	}

	for _, zipcode := range zipcodes {
		countyFipsCodes = append(countyFipsCodes, getLastThreeChars(zipcode.CountyFIPS))

		for _, altCounty := range zipcode.AlternateCounties {
			countyFipsCodes = append(countyFipsCodes, getLastThreeChars(altCounty.CountyFIPS))
		}
	}

	return countyFipsCodes
}

func getLastThreeChars(s string) string {
	if len(s) >= 3 {
		return s[len(s)-3:]
	}
	return ""
}
