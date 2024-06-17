package smarty

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
)

type ZipcodeRequest struct {
	Zipcode string `json:"zipcode"`
}

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
