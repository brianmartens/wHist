package main

import (
	"fmt"
	"io/ioutil"

	"net/http"
	"os"
	"time"
)

type Dataset struct {
	uid     string
	mindate *time.Time
	maxdate *time.Time
}

const NOAA_STATION string = "https://www.ncdc.noaa.gov/cdo-web/api/v2/stations"
const NOAA_TEST_STATION string = "GHCND:US1NCBC0005"
const NOAA_DATA string = "https://www.ncdc.noaa.gov/cdo-web/api/v2/data?"
const DATASET string = "GHCND"

func getStations() {

	client := &http.Client{}

	req, err := http.NewRequest("GET", NOAA_DATA+"stationid="+NOAA_TEST_STATION+"&datasetid=GHCND&startdate=2012-06-10&enddate=2012-09-10&limit=5", nil)
	if err != nil {
		fmt.Println(err)
	}

	req.Header.Add("Token", NOAA_API)

	resp, err := client.Do(req)
	if err != nil {
		fmt.Println(err)
	}

	bytes, err := ioutil.ReadAll(resp.Body)
	if err != nil {
		fmt.Println(err)
	}

	fmt.Println(string(bytes))

	ioutil.WriteFile("C:\\Users\\bobno\\Desktop\\wHist\\src\\DataMining\\stationList.json", bytes, os.ModeAppend)
}
