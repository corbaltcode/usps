package processsmarty

import (
	"encoding/json"
	"fmt"
)

type SmartyResponse struct {
	InputIndex int `json:"input_index"`
	CityStates []struct {
		City              string `json:"city"`
		StateAbbreviation string `json:"state_abbreviation"`
		State             string `json:"state"`
		MailableCity      bool   `json:"mailable_city"`
	} `json:"city_states"`
	Zipcodes []struct {
		Zipcode           string  `json:"zipcode"`
		ZipcodeType       string  `json:"zipcode_type"`
		DefaultCity       string  `json:"default_city"`
		CountyFIPS        string  `json:"county_fips"`
		CountyName        string  `json:"county_name"`
		StateAbbreviation string  `json:"state_abbreviation"`
		State             string  `json:"state"`
		Latitude          float64 `json:"latitude"`
		Longitude         float64 `json:"longitude"`
		Precision         string  `json:"precision"`
		AlternateCounties []struct {
			CountyFIPS        string `json:"county_fips"`
			CountyName        string `json:"county_name"`
			StateAbbreviation string `json:"state_abbreviation"`
			State             string `json:"state"`
		} `json:"alternate_counties"`
	} `json:"zipcodes"`
}

type ZipcodeResult struct {
	Zipcode         string
	USPSCounties    []string
	SmartyFIPS      []string
	Inconsistencies int
}

func ProcessSmartyResponse(responseBody []byte, zipToCounties map[string][]string, yield func(ZipcodeResult)) error {
	var smartyResponses []SmartyResponse
	err := json.Unmarshal(responseBody, &smartyResponses)
	if err != nil {
		return fmt.Errorf("failed to unmarshal response: %w", err)
	}

	for _, response := range smartyResponses {
		for _, zipcode := range response.Zipcodes {
			result := ZipcodeResult{
				Zipcode:      zipcode.Zipcode,
				USPSCounties: zipToCounties[zipcode.Zipcode],
			}

			smartyFIPS := []string{zipcode.CountyFIPS[len(zipcode.CountyFIPS)-3:]}
			for _, altCounty := range zipcode.AlternateCounties {
				smartyFIPS = append(smartyFIPS, altCounty.CountyFIPS[len(altCounty.CountyFIPS)-3:])
			}
			result.SmartyFIPS = smartyFIPS

			inconsistencies := calculateInconsistencies(result.USPSCounties, result.SmartyFIPS)
			result.Inconsistencies = inconsistencies

			yield(result)
		}
	}

	return nil
}

func calculateInconsistencies(uspsFIPS, smartyFIPS []string) int {
	uspsSet := make(map[string]struct{})
	for _, fips := range uspsFIPS {
		uspsSet[fips] = struct{}{}
	}

	inconsistencies := 0
	for _, fips := range smartyFIPS {
		if _, found := uspsSet[fips]; !found {
			inconsistencies++
		}
	}

	return inconsistencies
}
