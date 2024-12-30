package main

import (
	"fmt"
	"os"
	"path/filepath"
	"database/sql"
	"github.com/corbaltcode/usps/zip4"
	"github.com/corbaltcode/usps/citystate"
	_ "github.com/mattn/go-sqlite3" // sqlite driver
)

const BATCH_SIZE = 500000

const zip4CreateTableQuery = `CREATE TABLE IF NOT EXISTS zip4_data(
								ZipCode TEXT NOT NULL,
								RecordTypeCode TEXT,
								StateAbbreviation TEXT,
								CountyNumber TEXT,
								Plus4LowNumber TEXT,
								Plus4HighNumber TEXT)`
const zip4InsertQuery = `INSERT INTO zip4_data(ZipCode,RecordTypeCode,StateAbbreviation,CountyNumber,Plus4LowNumber,Plus4HighNumber) VALUES(?,?,?,?,?,?)`


const citystateCreateTableQuery = `CREATE TABLE IF NOT EXISTS city_state(
									CopyrightDetailCode TEXT,
									ZipCode TEXT NOT NULL,
									CityStateKey TEXT,
									ZipClassificationCode TEXT,
									CityStateName TEXT,
									CityStateNameAbbreviation TEXT,
									CityStateNameFacilityCode TEXT,
									CityStateMailingNameIndicator TEXT,
									PreferredLastLineCityStateKey TEXT,
									PreferredLastLineCityStateName TEXT,
									CityDeliveryIndicator TEXT,
									CarrierRouteRateSortation TEXT,
									UniqueZipNameIndicator TEXT,
									FinanceNumber TEXT,
									StateAbbreviation TEXT,
									CountyNumber TEXT,
									CountyName TEXT)`
const citystateInsertQuery = `INSERT INTO city_state(CopyrightDetailCode,ZipCode,CityStateKey,ZipClassificationCode,CityStateName,CityStateNameAbbreviation,CityStateNameFacilityCode,CityStateMailingNameIndicator,PreferredLastLineCityStateKey,PreferredLastLineCityStateName,CityDeliveryIndicator,CarrierRouteRateSortation,UniqueZipNameIndicator,FinanceNumber,StateAbbreviation,CountyNumber,CountyName) VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)`

func main() {
	if len(os.Args) < 2 {
		fmt.Fprintf(os.Stderr, "usage: %v <db-name>\n", filepath.Base(os.Args[0]))
		os.Exit(1)
	}

	dbName := os.Args[1]

	db, err := sql.Open("sqlite3", dbName)
	if err != nil {
		panic(err)
	}
	defer db.Close()

	err = SeedZip4Data(db)
	if err != nil {
		panic(err)
	}

	err = SeedCityStateData(db)
	if err != nil {
		panic(err)
	}
}

func SeedZip4Data(db *sql.DB) error {
	var zip4Data []zip4.Zip4Detail
	count := 1
	_, err := db.Exec(zip4CreateTableQuery)
	if err != nil {
		return err
	}

	tx, err := db.Begin()
	if err != nil {
		return err
	}

	err = zip4.ReadZip4FromZip4Tar("zip4natl.tar", mustGetenv("ZIP4_PWD"), func(detail zip4.Zip4Detail){
		zip4Data = append(zip4Data, detail)
		if count%BATCH_SIZE == 0 {
			for i := 0; i < len(zip4Data); i++ {
				params := getParameters(zip4Data[i])
				_, err = tx.Exec(zip4InsertQuery, params...)
				if err != nil {
					panic(err)
				}
			}

			zip4Data = []zip4.Zip4Detail{}
		}
		
		count++
	})
	if err != nil {
		return err
	}

	if len(zip4Data) != 0 {
		for i := 0; i < len(zip4Data); i++ {
			params := getParameters(zip4Data[i])
			_, err = tx.Exec(zip4InsertQuery, params...)
			if err != nil {
				return err
			}
		}
	}

	err = tx.Commit()
	if err != nil {
		return err
	}

	return nil
}

func SeedCityStateData(db *sql.DB) error {
	var citystateData []citystate.CityStateDetail

	_, err := db.Exec(citystateCreateTableQuery)
	if err != nil {
		return err
	}

	tx, err := db.Begin()
	if err != nil {
		return err
	}

	err = zip4.ReadCityStateFromZip4Tar("zip4natl.tar", mustGetenv("CITYSTATE_PWD"), func(detail citystate.CityStateDetail){
		citystateData = append(citystateData, detail)
		if len(citystateData)%BATCH_SIZE == 0 && len(citystateData) != 0 {
			for i := 0; i < len(citystateData); i++ {
				params := getParameters(citystateData[i])
				_, err = tx.Exec(citystateInsertQuery, params...)
				if err != nil {
					panic(err)
				}
			}

			citystateData = []citystate.CityStateDetail{}
		}
		
	})
	if err != nil {
		return err
	}

	if len(citystateData) != 0 {
		for i := 0; i < len(citystateData); i++ {
			params := getParameters(citystateData[i])
			_, err = tx.Exec(citystateInsertQuery, params...)
			if err != nil {
				return err
			}
		}
	}

	err = tx.Commit()
	if err != nil {
		return err
	}

	return nil
}

func getParameters(detail interface{}) []interface{} {
	switch params := detail.(type) {
	case citystate.CityStateDetail:
		val := []interface{}{
			params.CopyrightDetailCode,
			params.ZipCode,
			params.CityStateKey,
			params.ZipClassificationCode,
			params.CityStateName,
			params.CityStateNameAbbreviation,
			params.CityStateNameFacilityCode,
			params.CityStateMailingNameIndicator,
			params.PreferredLastLineCityStateKey,
			params.PreferredLastLineCityStateName,
			params.CityDeliveryIndicator,
			params.CarrierRouteRateSortation,
			params.UniqueZipNameIndicator,
			params.FinanceNumber,
			params.StateAbbreviation,
			params.CountyNumber,
			params.CountyName,
		}
		return val
	case zip4.Zip4Detail:
		val := []interface{}{
			params.ZipCode,
			params.RecordTypeCode,
			params.StateAbbreviation,
			params.CountyNumber,
			params.Plus4LowNumber,
			params.Plus4HighNumber,
		}
		return val
	}

	return nil
}

func mustGetenv(key string) string {
	v, ok := os.LookupEnv(key)
	if !ok {
		panic(fmt.Sprintf("missing env var: %v", key))
	}
	return v
}