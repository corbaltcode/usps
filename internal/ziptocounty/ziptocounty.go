package ziptocounty

import (
	"fmt"
	"sort"

	"github.com/corbaltcode/usps/internal/smarty"
	"github.com/corbaltcode/usps/zip4"
)

type ZIPCountyDiff struct {
	Zipcode       string
	USPSFips      []string
	SmartyFips    []string
	MismatchCount int
	ErrorMessage  string
}

func GenerateSmartyUSPSDiff(zipcode string, smartyResponse smarty.Response, countyMapping map[string][]string) ZIPCountyDiff {
	errorMessage := ""

	if smartyResponse.Status != "" {
		errorMessage = fmt.Sprintf("ZIP code input: %s, Status response: %s, Reason: %s", zipcode, smartyResponse.Status, smartyResponse.Reason)
	}
	uspsFips := countyMapping[zipcode]
	smartyFips := smarty.ExtractCountyFipsCodes(smartyResponse.Zipcodes)

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

func CollectUSPSZip4Details(tarName, zipPassword string) (map[string][]string, error) {
	zipToCounty := make(map[string][]string)

	yield := func(detail zip4.Zip4Detail) {
		zip := detail.ZipCode
		county := detail.CountyNumber

		// Check if this county code has already been seen for the current zip.
		found := false
		for _, c := range zipToCounty[zip] {
			if c == county {
				found = true
				break
			}
		}

		// If we haven't seen this county code for the current zip, append it.
		if !found {
			zipToCounty[zip] = append(zipToCounty[zip], county)
		}
	}

	err := zip4.ReadZip4FromZip4Tar(tarName, zipPassword, yield)
	if err != nil {
		return nil, fmt.Errorf("error processing ZIP+4 data: %v", err)
	}

	return zipToCounty, nil
}
