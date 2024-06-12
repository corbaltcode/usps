package smartyresponseprocessing

import (
	"sort"

	"golang.org/x/exp/slices"
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
	Zipcode     string
	USPSFips    []string
	SmartyFips  []string
	HasMismatch bool
}

func ProcessSmartyResponse(responseBody []SmartyResponse, zipToCounties map[string][]string, yield func(ZipcodeResult)) error {
	for _, response := range responseBody {
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

			result.HasMismatch = slicesDiffer(result.USPSFips, result.SmartyFips)

			yield(result)
		}
	}

	return nil
}

func slicesDiffer(uspsFIPS, smartyFIPS []string) bool {
	sort.Strings(uspsFIPS)
	sort.Strings(smartyFIPS)
	return !slices.Equal(uspsFIPS, smartyFIPS)
}
