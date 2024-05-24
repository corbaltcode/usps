package counties

import (
	"fmt"

	"github.com/corbaltcode/usps/zip4"
)

func CollectZip4Details(tarName, zipPassword string) (map[string][]string, error) {
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
