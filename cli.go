package main

import (
	"bufio"
	"database/sql"
	"encoding/csv"
	"flag"
	"fmt"
	"io"
	"os"
	"time"

	_ "github.com/lib/pq"
	// MIT license so its fine to use.
	"github.com/montanaflynn/stats"
)

// Given this is only suppose to run in docker, the db info is hardcoded. This would need to be
// refactored if it were to be used in a different enviorment.
const (
	// host     = "localhost"
	host     = "db"
	port     = 5432
	user     = "postgres"
	password = "password"
	dbname   = "homework"
)

// struct to allow easy passing of input to workers
type Input struct {
	hostname  string
	starttime time.Time
	endtime   time.Time
}

// struct to allow easy return of stats from workers
type Stats struct {
	totalQueries int64
	totalTime    time.Duration
	queryTimes   []time.Duration
}

// query template for workers. We may want to move this somewhere else in the future.
const QUERY_TEMPLATE = "SELECT min(usage), max(usage) from cpu_usage where host='%s' and ts >= '%s' and ts <= '%s'"
const NANO_TO_MS = 1_000_000

func main() {

	fileName := flag.String("file", "input/query_params.csv", "location of input file")
	useStdIn := flag.Bool("stdin", false, "set to true if piping input through stdin")
	numThreads := *flag.Int("workers", 1, "number of wocker threads")

	addData := flag.Bool("add_data", false, "if true adds the expected data and returns")

	flag.Parse()

	// See comment on addDataToDB
	if *addData {
		addDataToDB()
		return
	}

	// validate input
	if numThreads <= 0 {
		fmt.Println("workers must be a positive number")
		return
	}

	// The requirement was that 1) use a variable number of workers and 2) each hostname should use exactly 1 worker. The below code
	// creates a map to assign each hostname to a worker and does a roundrobin assignment to workers. If hostnames were not sticky to
	// a given worker we could instead use a worker pool.
	hostNameAssignment := make(map[string]int)
	nextAssignement := 0

	// We need a channel for each worker to pass work and a channel to return stats.
	inputChans := make([]chan Input, numThreads)
	statsChan := make([]chan Stats, numThreads)
	for i := 0; i < numThreads; i++ {
		inputChans[i] = make(chan Input)
		statsChan[i] = make(chan Stats)
		go workerFunction(inputChans[i], statsChan[i])
	}

	var r *csv.Reader
	processStartTime := time.Now()
	if *useStdIn {
		r = csv.NewReader(bufio.NewReader(os.Stdin))
	} else {
		reader, err := os.Open(*fileName)
		if err != nil {
			fmt.Printf("Unable to open input file %s\nError: %s\n", *fileName, err)
			return
		}
		defer reader.Close()
		r = csv.NewReader(reader)
	}

	// Read first row and drop it since its the header.
	firstRow, err := r.Read()
	if err != nil {
		fmt.Printf("error reading input file %s\n", err.Error())
		return
	} else if firstRow[0] != "hostname" {
		fmt.Println("Expected first time of input file to be header")
		return
	}

	var input Input

	stop := false
	for !stop {
		row, err := r.Read()
		if err == io.EOF {
			stop = true
		} else if err != nil {
			fmt.Printf("Error reading file: %s\n", err)
			return
		} else {

			input.hostname = row[0]
			input.starttime, err = time.Parse(time.DateTime, row[1])
			if err != nil {
				fmt.Printf("Error parsing date: %s\nError: %s\n", row[1], err)
				return
			}
			input.endtime, err = time.Parse(time.DateTime, row[2])
			if err != nil {
				fmt.Printf("Error parsing date: %s\nError: %s\n", row[2], err)
				return
			}

			// Check if hostname has been assigned. If it hasn't assign it.
			assignment, found := hostNameAssignment[input.hostname]
			if !found {
				assignment = nextAssignement
				nextAssignement = (nextAssignement + 1) % numThreads
				hostNameAssignment[input.hostname] = assignment
			}
			inputChans[assignment] <- input
		}
	}

	// Close each channel
	for i := 0; i < numThreads; i++ {
		close(inputChans[i])
	}

	// Get stats from worker and combine them
	var statsAgg Stats
	for i := 0; i < numThreads; i++ {
		tempStats := <-statsChan[i]
		statsAgg.totalQueries = tempStats.totalQueries
		statsAgg.totalTime = tempStats.totalTime
		statsAgg.queryTimes = tempStats.queryTimes
	}

	totalProcessTime := time.Since(processStartTime)

	// Aggregate stats using library.
	// I included P90 and P95 because I have seen that they are often very useful when looking into performance.
	// Ignore errors since we know the incoming data is okay and its not worth the extra code.
	data := stats.LoadRawData(statsAgg.queryTimes)
	min, _ := data.Min()
	median, _ := data.Median()
	mean, _ := data.Mean()
	p90, _ := data.Percentile(90)
	p95, _ := data.Percentile(95)
	max, _ := data.Max()

	// output stats
	fmt.Printf("Total Runtime : %d ms\n", totalProcessTime.Milliseconds())
	fmt.Printf("Total Queries: %d\n", statsAgg.totalQueries)
	fmt.Printf("Total Query Execution time : %d ms\n", statsAgg.totalTime.Milliseconds())
	fmt.Printf("Average Execution time : %.2f ms\n", mean/NANO_TO_MS)
	fmt.Printf("Min    : %.2f ms\n", min/NANO_TO_MS)
	fmt.Printf("Median : %.2f ms\n", median/NANO_TO_MS)
	fmt.Printf("P90    : %.2f ms\n", p90/NANO_TO_MS)
	fmt.Printf("P95    : %.2f ms\n", p95/NANO_TO_MS)
	fmt.Printf("Max    : %.2f ms\n", max/NANO_TO_MS)
}

func workerFunction(inputChan <-chan Input, statsChan chan<- Stats) {
	// Create stats setup
	var stats Stats

	// Create db connection: Given this is for a CLI each worker can create and reuse its own connection. If this grows we might want to use a
	// db connection pool of some sort.
	psqlInfo := fmt.Sprintf("host=%s port=%d user=%s "+"password=%s dbname=%s sslmode=disable", host, port, user, password, dbname)
	db, err := sql.Open("postgres", psqlInfo)
	if err != nil {
		fmt.Printf("Unable to connect to db. Please check settings match db\nError: %s\n", err)
		panic(err)
	}
	defer db.Close()

	// Warm up db connection to improve max process time.
	err = db.Ping()
	if err != nil {
		fmt.Printf("Unable to ping db. Please check settings match db\nError: %s\n", err)
		panic(err)
	}

	// spin until done
	for {
		var start time.Time
		var length time.Duration
		input, more := <-inputChan
		if more {
			start = time.Now()
			row := db.QueryRow(fmt.Sprintf(QUERY_TEMPLATE, input.hostname, input.starttime.Format(time.DateTime), input.endtime.Format(time.DateTime)))
			length = time.Since(start)

			// Scan the row and check for error but we don't care about the specific result at this time.
			var min *float64
			var max *float64
			err = row.Scan(&min, &max)
			if err != nil {
				fmt.Println("Unable to scan row correctly")
				panic(err)
			}
			stats.totalQueries += 1
			stats.queryTimes = append(stats.queryTimes, length)
			stats.totalTime += length
		} else {
			// return stats and finish
			statsChan <- stats
			return
		}

	}
}

// I was developing on a linux VM on my windows laptop and ran into problems getting a docker image with psql fitting on the virtual disk
// instead of reimaging the OS on a bigger disk to be able to use psql to inport the input csv I decided to do it this way. There is probably a better/faster
// way to implement the inport. It takes a bit of time to run but it works.
func addDataToDB() {
	psqlInfo := fmt.Sprintf("host=%s port=%d user=%s "+"password=%s dbname=%s sslmode=disable", host, port, user, password, dbname)
	db, err := sql.Open("postgres", psqlInfo)
	if err != nil {
		fmt.Printf("Unable to connect to db. Please check settings match db\nError: %s\n", err)
		panic(err)
	}
	defer db.Close()

	reader, err := os.Open("/app/input/cpu_usage.csv")
	if err != nil {
		fmt.Printf("Unable to open input file %s\nError: %s\n", "/app/input/cpu_usage.csv", err)
		return
	}
	defer reader.Close()
	r := csv.NewReader(reader)

	records, err := r.ReadAll()
	if err != nil {
		fmt.Printf("error reading input file %s\n", err.Error())
		return
	} else if records[0][0] != "ts" {
		fmt.Println("Expected first time of input file to be header")
		return
	}

	counter := 0
	queryString := ""
	for i, row := range records {
		if i == 0 {
			if row[0] != "ts" {
				fmt.Println("Expected first time of input file to be header")
			}
		} else {
			if counter == 0 {
				queryString = "INSERT INTO cpu_usage VALUES ("
			}
			if counter < 10000 {
				queryString += fmt.Sprintf("'%s', '%s', '%s'),(", row[0], row[1], row[2])
				counter += 1
			} else {
				queryString += fmt.Sprintf("'%s', '%s', '%s')", row[0], row[1], row[2])
				fmt.Printf("executing query %d\n", i)
				_, err := db.Exec(queryString)
				if err != nil {
					panic(err)
				}
				counter = 0
			}
			_, err := db.Exec(fmt.Sprintf("INSERT INTO cpu_usage VALUES ('%s', '%s', %s)", row[0], row[1], row[2]))
			if err != nil {
				panic(err)
			}
		}
	}
}
