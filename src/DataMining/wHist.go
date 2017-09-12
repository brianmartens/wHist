package main

import (
	"compress/gzip"
	"encoding/csv"
	"encoding/json"
	"errors"
	"fmt"
	"io/ioutil"
	"net/http"
	"os"
	"strconv"
	"strings"
	"time"
)

var extraloc = map[string]string{
	"ES": "/airport/LEEC",
	"GU": "/airport/PGUM",
	"IT": "/airport/LICZ",
	"JP": "/airport/ROAH",
	"NO": "/airport/ENVA",
	"PI": "/airport/RPUH",
	"PR": "/airport/TJSJ",
	"RO": "/airport/LRTZ",
}

const GeoLoc, ST, Lat, Lon int = 0, 1, 2, 3
const WunAPIKEY string = ""
const GooAPIKEY string = ""
const NOAA_API string = ""
const WunHistBase string = "https://www.wunderground.com"
const wunHQ string = "DailyHistory.html?"
const wunSearchBase string = "https://www.wunderground.com/cgi-bin/findweather/getForecast?query="
const CSE string = "mysearch-1494207888632"
const CSE2 string = "954822024567"
const tableBegin string = "<div id=\"observations_details\" class=\"high-res\" >"
const tableEnd string = "<div class=\"obs-table-footer\">"
const tableRow string = "<tr class=\"no-metars\">"
const ROOT string = "/home/brian/wHist"
const rowFilter string = "<span class=\"wx-data\"><span class=\"wx-value\">"

type CalendarDay struct {
	Headers []string   "json:\"headers\""
	Data    [][]string "json:\"data\""
}

func (cday *CalendarDay) getHeaders() []string {
	return cday.Headers
}

func getLocations() map[string][]string {
	fileBytes, err := ioutil.ReadFile(ROOT + "/GEO_LOCATIONS.csv")
	if err != nil {
		fmt.Println(err)
	}
	fileStr := string(fileBytes)
	table := make(map[string][]string)
	rCsv := csv.NewReader(strings.NewReader(fileStr))
	_, SkpHdr := rCsv.Read()
	if SkpHdr != nil {
		fmt.Println(SkpHdr)
	}
	for {
		record, err := rCsv.Read()
		if err != nil {
			break
		}
		var correct string = record[0]
		strings.Replace(correct, " ", "_", -1)
		table[correct] = record
	}
	return table
}

func getDate(url, returnType string) string {
	var urlDate string
	histSplit := strings.Split(url, "/history/")
	if len(histSplit) > 1 {
		slashSplit := strings.Split(histSplit[1], "/")
		for i := 1; i < 5; i++ {
			if length := len(slashSplit); length >= i {
				if i == 2 || i == 3 {
					urlDate += slashSplit[i] + returnType
				} else if i == 4 {
					urlDate += slashSplit[i]
				}
			}
		}
	}
	return urlDate
}

func writeGzip(fileName string, fileBytes []byte, filemode os.FileMode) error {

	file, err := os.Create(fileName + ".gz")
	if err != nil {
		return err
	}

	file.Chmod(filemode)
	gwriter := gzip.NewWriter(file)
	if _, err := gwriter.Write(fileBytes); err != nil {
		return err
	}

	if err := gwriter.Close(); err != nil {
		return err
	}

	if err := file.Close(); err != nil {
		return err
	}

	return nil
}

func readGzip(fileName string) ([]byte, error) {

	file, err := os.OpenFile(fileName, os.O_RDONLY, 0755)
	if err != nil {
		return nil, err
	}

	greader, err := gzip.NewReader(file)
	if err != nil {
		return nil, err
	}

	b, err := ioutil.ReadAll(greader)
	if err != nil {
		return nil, err
	}

	if err := greader.Close(); err != nil {
		return nil, err
	}

	if err := file.Close(); err != nil {
		return nil, err
	}

	return b, nil
}

func getUrl(Geolocation string) string {
	if gzipBytes, err := readGzip(ROOT + "/data/" + Geolocation + "/url.txt.gz"); err != nil {
		if fileBytes, err := ioutil.ReadFile(ROOT + "/data/" + Geolocation + "/url.txt"); err != nil {
			// uncompressed url file doesn't exist, so we need to download it
			return ""
		} else {
			// uncompressed url file exists, so we will attempt to gzip it
			if err := writeGzip(ROOT+"/data/"+Geolocation+"/url.txt", fileBytes, 0755); err != nil {
				return ""
			}
			return string(fileBytes)
		}
	} else {
		os.Remove(ROOT + "/data/" + Geolocation + "/url.txt")
		return string(gzipBytes)
	}
}

func getWeather(url, Geo string) error {
	// this returns an error if we cannot find the file corresponding to the url and Geo
	var sDate string = getDate(url, "-")
	var sFile string = ROOT + "/data/" + Geo + "/html/" + sDate + ".html"
	// if err == nil then we have the file on hand, otherwise we need to check for an uncompressed file
	if _, err := readGzip(sFile + ".gz"); err == nil {
		return nil

	}
	if _, err := ioutil.ReadFile(sFile); err != nil {
		return errors.New(sDate + " not found for " + Geo + "\nerr: " + err.Error())

	}
	return nil

}

func parseWeather(bytes []byte) (*CalendarDay, []byte) {

	var sHtml string = string(bytes)
	var htmlTable []byte
	start := strings.Split(sHtml, tableBegin)
	weather := CalendarDay{}

	if len(start) > 1 {
		// fullTable contains all table data and tHead contains just the header row
		fullTable := strings.Split(start[1], tableEnd)
		tHead := strings.Split(start[1], "</thead>")
		//Table Header
		if len(tHead) > 0 {
			iThr := strings.Split(tHead[0], "<tr>")
			if len(iThr) > 1 {
				thrBody := strings.Split(iThr[1], "</tr>")
				if len(thrBody) > 0 {
					thrElements := strings.Split(thrBody[0], "<th>")
					if len(thrElements) > 1 {
						var header []string

						for i := 1; i < len(thrElements); i++ {
							element := strings.Split(thrElements[i], "<")

							if len(element) > 0 {
								header = append(header, strings.Replace(element[0], " ", "", -1))
							}
						}
						weather.Headers = header
					}
				}
			}
		}
		// Parse data
		if len(fullTable) > 0 {
			htmlTable = []byte(fullTable[0])
			tRows := strings.Split(fullTable[0], tableRow)
			var aData [][]string

			for _, row := range tRows[1:] {

				rowData := strings.Split(row, "<td >")
				var aRow []string
				for _, data := range rowData {

					cut := strings.Split(data, "</")
					var rowVal string
					if len(cut) > 0 {
						backCut := strings.Split(cut[0], ">")

						if len(backCut) > 0 {
							rowVal = backCut[len(backCut)-1]
						}
					}

					if len(cut) > 1 {
						semiSplit := strings.Split(cut[1], ";")
						if len(semiSplit) > 1 {
							rowVal += " " + semiSplit[len(semiSplit)-1]
						}
					}
					rowVal = strings.Replace(rowVal, "\n", "", -1)
					aRow = append(aRow, rowVal)

				}
				aData = append(aData, aRow)

			}
			weather.Data = aData

		}

	}
	return &weather, htmlTable

}

func createWeather(client *http.Client, url, Geolocation string) (error, string) {

	urlSplit := strings.Split(url, "?")
	var subdir string = ROOT + "/data/" + Geolocation
	if len(urlSplit) > 0 {
		sDate := getDate(urlSplit[0], "-")
		// getWeather will attempt to find the gzip file, or create it if it can only find the uncompressed file
		// in the case that it finds any files, it will return a nil error value as the data does exist already.
		if err := getWeather(urlSplit[0], Geolocation); err != nil {
			//fmt.Println(sDate + " not on file for " + Geolocation + ": Downloading file now\nlog: " + err.Error())

		} else {
			//otherwise the file exists, no action needed
			return nil, "file"

		}

		if resp, err := client.Get(urlSplit[0]); err != nil {
			return errors.New("Download failed: url = " + urlSplit[0] + "\nerr: " + err.Error()), ""

		} else {
			if rBytes, err := ioutil.ReadAll(resp.Body); err != nil {
				return errors.New("Error reading response body - status: \n" + resp.Status + "\nerr: " + err.Error()), ""

			} else {
				var wBytes [][]byte
				weatherDay, html := parseWeather(rBytes)
				//append the html bytes into the wBytes 'collection'
				wBytes = append(wBytes, html)
				if jsonBytes, err := json.Marshal(weatherDay); err != nil {
					return errors.New("Error unmarshalling weatherDay bytes{" + string(jsonBytes) + "}\nerr: " + err.Error()), ""

				} else {
					//append the json bytes into wBytes
					wBytes = append(wBytes, jsonBytes)
					// Now attempt to write the gzip files for both html and json directories
					var sTypes []string = []string{"html", "json"}
					for i := range sTypes {
						if err := writeGzip(subdir+"/"+sTypes[i]+"/"+sDate+"."+sTypes[i], wBytes[i], 0755); err != nil {
							return errors.New("Error writing gzip " + sTypes[i] + " file for #" + sDate + "# in subdir \"" + subdir + "\"\nerr: " + err.Error()), ""

						}

					}

				}

			}

		}

	}
	return nil, "net"
}

func createUrl(client *http.Client, Geolocation string, record []string) string {

	if ok := getUrl(Geolocation); ok != "" {
		return ok
	}

	var url string = wunSearchBase + record[Lat] + "," + record[Lon]

	search, err := client.Get(url)
	if err != nil {
		fmt.Println(err)
	}
	sbytes, err := ioutil.ReadAll(search.Body)
	if err != nil {
		fmt.Println(err)
	}
	url = ""
	initSplit := strings.Split(string(sbytes), "id=\"city-nav-history\"")
	if len(initSplit) > 0 {
		href := strings.Split(initSplit[0], "<li><a href=\"/history/airport")
		if len(href) > 1 {
			qSplit := strings.Split(href[1], "?")
			if len(qSplit) > 0 {
				url = "/history/airport" + qSplit[0]
			}
		}
	}
	url = WunHistBase + url

	if err := os.MkdirAll(ROOT+"/data", 0755); err != nil {
		fmt.Println(err)
	}

	if err := os.MkdirAll(ROOT+"/data/"+Geolocation, 0755); err != nil {
		fmt.Println(err)
	}

	if err := os.MkdirAll(ROOT+"/data/"+Geolocation+"/html", 0755); err != nil {
		fmt.Println(err)
	}

	if err := os.MkdirAll(ROOT+"/data/"+Geolocation+"/json", 0755); err != nil {
		fmt.Println(err)
	}

	// write URL to gzip file
	if err := writeGzip(ROOT+"/data/"+Geolocation+"/url.txt", []byte(url), 0755); err != nil {
		fmt.Println(err)
	}

	fmt.Println(Geolocation + " url created...")
	return url
}

func createRoutine(c chan string, client *http.Client, urlMap map[string]string, Geolocation string) {
	//beginning date and ending date as of right now
	startTime := time.Date(2004, time.January, 1, 0, 0, 0, 0, time.Local)
	endTime := time.Now()
	// temporary url that we will use for editing
	var urlTemp string = urlMap[Geolocation]
	// total files downloaded vs read
	iNet, iFile := 0, 0
	// get the date from the url and store as a string value (we will use this with the Replace() function)
	sOld := getDate(urlTemp, "/")
	for {
		if startTime.After(endTime) {
			break

		}
		sNext := strconv.Itoa(startTime.Year()) + "/" + strconv.Itoa(int(startTime.Month())) + "/" + strconv.Itoa(startTime.Day())
		urlTemp = strings.Replace(urlTemp, sOld, sNext, -1)
		if err, sArg := createWeather(client, urlTemp, Geolocation); err != nil {
			fmt.Println("CreateWeather " + urlTemp + " for " + Geolocation + " failed.\nerr: " + err.Error())

		} else if sArg != "" {
			if sArg == "file" {
				iFile++

			} else {
				iNet++

			}

		}
		sOld = sNext
		startTime = startTime.Add(time.Duration(24) * time.Hour)

	}
	c <- (Geolocation + " Complete\n" + strconv.Itoa(iFile) + " files read\n" + strconv.Itoa(iNet) +
		" files downloaded\nElapsed: " + time.Now().Sub(endTime).String())

}

func main() {
	client := &http.Client{}
	geoMap := getLocations()
	urlMap := make(map[string]string)
	for geo, record := range geoMap {
		urlMap[geo] = createUrl(client, geo, record)

	}
	c := make(chan string)
	var threads int = 0
	for Geolocation, _ := range geoMap {
		subClient := &http.Client{}
		threads++
		go createRoutine(c, subClient, urlMap, Geolocation)

	}
	for {
		if threads <= 0 {
			fmt.Println("Done with all Geolocations")
			break

		}
		var input string = <-c
		if strings.Contains(input, "Complete") {
			fmt.Println(input)
			threads--

		}

	}

}
