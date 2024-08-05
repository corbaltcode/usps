package main

import (
	"flag"
	"fmt"
	"os"

	"github.com/corbaltcode/usps/internal/smarty"
	"github.com/corbaltcode/usps/internal/ziptocounty"
)

func main() {
	tarName := flag.String("tar", "", "Name of the tar file")
	zipCode := flag.String("zip", "", "Single zip to look up")
	flag.Parse()

	if *tarName == "" || *zipCode == "" {
		fmt.Fprintf(os.Stderr, "Error: Missing required parameters\nUsage: %s -tar <tar_file_name> -zip <single_zip_code>\n", os.Args[0])
		os.Exit(1)
	}

	zipPassword := mustGetenv("ZIP_PASSWORD")
	authId := mustGetenv("AUTH_ID")
	authToken := mustGetenv("AUTH_TOKEN")
	client := smarty.NewClient(authId, authToken)

	fmt.Printf("Extracting USPS zip data from %s and mapping %v to corresponding USPS counties...\n", *tarName, *zipCode)

	zipToCounty, err := ziptocounty.CollectUSPSZip4Details(*tarName, zipPassword)
	if err != nil {
		fmt.Fprintf(os.Stderr, "Failed to collect details: %v\n", err)
		os.Exit(1)
	}

	counties, found := zipToCounty[*zipCode]
	if !found || len(counties) == 0 {
		fmt.Fprintf(os.Stderr, "Error: No counties found for ZIP Code: %s\n", *zipCode)
		os.Exit(1)
	}

	zipResponse, err := client.QueryBatch([]string{*zipCode})
	if err != nil {
		fmt.Fprintf(os.Stderr, "Error querying Smarty API: %v\n", err)
		os.Exit(1)
	}

	for _, response := range zipResponse {
		diff := ziptocounty.GenerateSmartyUSPSDiff(*zipCode, response, zipToCounty)

		fmt.Printf("ZIP Code Differences for %s:\n", *zipCode)
		fmt.Printf("  - USPS FIPS Codes:   %v\n", diff.USPSFips)
		fmt.Printf("  - Smarty FIPS Codes: %v\n", diff.SmartyFips)
		fmt.Printf("  - Mismatch Count:    %d\n", diff.MismatchCount)
		if diff.ErrorMessage != "" {
			fmt.Printf("  - Error:             %s\n", diff.ErrorMessage)
		} else {
			fmt.Printf("  - Status:            No errors detected.\n")
		}
		fmt.Println("--------------------------------------------------")
	}
}

func mustGetenv(key string) string {
	v, ok := os.LookupEnv(key)
	if !ok {
		panic(fmt.Sprintf("missing env var: %v", key))
	}
	return v
}
