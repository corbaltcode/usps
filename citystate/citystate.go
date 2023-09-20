package citystate

import (
	"io"
	"strings"
)

const cityStateRecordLength = 129

const (
	CityStateCopyrightDetailCodeAlias     = "A"
	CityStateCopyrightDetailCodeCopyright = "C"
	CityStateCopyrightDetailCodeDetail    = "D"
	CityStateCopyrightDetailCodeSeasonal  = "N"
	CityStateCopyrightDetailCodePOBoxOnly = "P"
	CityStateCopyrightDetailCodeSplit     = "Z"
)

type CityStateDetail struct {
	CopyrightDetailCode            string
	ZipCode                        string
	CityStateKey                   string
	ZipClassificationCode          string
	CityStateName                  string
	CityStateNameAbbreviation      string
	CityStateNameFacilityCode      string
	CityStateMailingNameIndicator  string
	PreferredLastLineCityStateKey  string
	PreferredLastLineCityStateName string
	CityDeliveryIndicator          string
	CarrierRouteRateSortation      string
	UniqueZipNameIndicator         string
	FinanceNumber                  string
	StateAbbreviation              string
	CountyNumber                   string
	CountyName                     string
}

func ReadCityStateFile(r io.Reader, yield func(CityStateDetail)) error {
	buf := make([]byte, cityStateRecordLength)

	for {
		if _, err := io.ReadFull(r, buf); err != nil {
			if err == io.EOF {
				break
			}

			return err
		}

		if buf[0] == CityStateCopyrightDetailCodeDetail[0] {
			detail, err := parseCityStateDetail(buf)

			if err != nil {
				return err
			}

			yield(detail)
		}
	}

	return nil
}

func parseCityStateDetail(buf []byte) (CityStateDetail, error) {
	s := string(buf)

	var d CityStateDetail
	d.CopyrightDetailCode = s[0:1]
	d.ZipCode = s[1:6]
	d.CityStateKey = s[6:12]
	d.ZipClassificationCode = s[12:13]
	d.CityStateName = strings.TrimSpace(s[13:41])
	d.CityStateNameAbbreviation = s[41:54]
	d.CityStateNameFacilityCode = s[54:55]
	d.CityStateMailingNameIndicator = s[55:56]
	d.PreferredLastLineCityStateKey = s[56:62]
	d.PreferredLastLineCityStateName = strings.TrimSpace(s[62:90])
	d.CityDeliveryIndicator = s[90:91]
	d.CarrierRouteRateSortation = s[91:92]
	d.UniqueZipNameIndicator = s[92:93]
	d.FinanceNumber = s[93:99]
	d.StateAbbreviation = s[99:101]
	d.CountyNumber = s[101:104]
	d.CountyName = strings.TrimSpace(s[104:129])

	return d, nil
}
