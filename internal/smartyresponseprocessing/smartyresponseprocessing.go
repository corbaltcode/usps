package smartyresponseprocessing

import (
	"encoding/json"
	"fmt"
)

type SmartyResponse struct {
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
	USPSFips        []string
	SmartyFips      []string
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
				Zipcode:  zipcode.Zipcode,
				USPSFips: zipToCounties[zipcode.Zipcode],
			}

			// Extract the last three digits of the Smarty Fips code, which represent the county.
			smartyFIPS := []string{zipcode.CountyFIPS[len(zipcode.CountyFIPS)-3:]}
			for _, altCounty := range zipcode.AlternateCounties {
				smartyFIPS = append(smartyFIPS, altCounty.CountyFIPS[len(altCounty.CountyFIPS)-3:])
			}
			result.SmartyFips = smartyFIPS

			inconsistencies := calculateInconsistencies(result.USPSFips, result.SmartyFips)
			result.Inconsistencies = inconsistencies

			yield(result)
		}
	}

	return nil
}

func calculateInconsistencies(uspsFIPS, smartyFIPS []string) int {
	uspsSet := make(map[string]bool)
	for _, fips := range uspsFIPS {
		uspsSet[fips] = true
	}

	inconsistencies := 0
	for _, fips := range smartyFIPS {
		if !uspsSet[fips] {
			inconsistencies++
		}
	}

	return inconsistencies
}
