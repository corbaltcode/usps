package counties

import (
	"fmt"

	"github.com/corbaltcode/usps/zip4"
)

func CollectZip4Details(tarName, zipPassword string) (map[string][]string, error) {
	var details []zip4.Zip4Detail

	yield := func(detail zip4.Zip4Detail) {
		details = append(details, detail)
	}

	err := zip4.ReadZip4FromZip4Tar(tarName, zipPassword, yield)
	if err != nil {
		return nil, fmt.Errorf("error processing ZIP+4 data: %v", err)
	}

	return mapZipToCounty(details), nil
}

func mapZipToCounty(details []zip4.Zip4Detail) map[string][]string {
	zipToCounty := make(map[string][]string)

	for _, detail := range details {
		zip := detail.ZipCode
		county := detail.CountyNumber

		// Check if the county number is already in the list for the zip.
		counties, exists := zipToCounty[zip]
		if exists {
			found := false
			for _, c := range counties {
				if c == county {
					found = true
					break
				}
			}
			if !found {
				zipToCounty[zip] = append(zipToCounty[zip], county)
			}
		} else {
			zipToCounty[zip] = []string{county}
		}
	}

	return zipToCounty
}
