package main

import (
	"database/sql"
	"encoding/csv"
	"fmt"
	_ "github.com/go-sql-driver/mysql"
	"io"
	"io/ioutil"
	"log"
	"os"
	"runtime"
	"strings"
	"sync"
	"time"
)

const (
	INPUT_PATH = "/home/chenzhi/Documents/test/throughput/split_equally_16/"
	QUERY      = "insert into task_events values (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)"
)

func currentTimeMilisecond() int64 {
	return time.Time.UnixNano(time.Now()) / 1000 / 1000
}

func simulate(wg *sync.WaitGroup, filename string) {
	fmt.Println(filename, "start at", time.Now())
	csvfile, err := os.Open(filename)
	if err != nil {
		fmt.Println(err)
		return
	}
	reader := csv.NewReader(csvfile)
	db, err := sql.Open("mysql", "root:123@/GoogleCluster")

	for {
		row, err := reader.Read()
		if err != nil {
			if err == io.EOF {
				break
			}
			fmt.Println(err)
			os.Exit(1)
		}
		_, err = db.Exec(QUERY, row[0], row[1], row[2], row[3], row[4], row[5], row[6], row[7], row[8], row[9], row[10], row[11], row[12])
		if err != nil {
			log.Fatal(err)
		}
	}
	db.Close()
	csvfile.Close()
	fmt.Println(filename, "end at", time.Now())
	wg.Done()

}

func main() {
	start_time := currentTimeMilisecond()
	runtime.GOMAXPROCS(runtime.NumCPU())
	var INPUT_FILE_LIST = make([]string, 0)
	contains, _ := ioutil.ReadDir(INPUT_PATH)
	for _, f := range contains {
		if !f.IsDir() && strings.HasSuffix(f.Name(), ".csv") {
			INPUT_FILE_LIST = append(INPUT_FILE_LIST, f.Name())
		}
	}
	wg := sync.WaitGroup{}
	wg.Add(len(INPUT_FILE_LIST))
	for _, name := range INPUT_FILE_LIST {
		go simulate(&wg, INPUT_PATH+name)
	}
	wg.Wait()
	fmt.Println("cost", currentTimeMilisecond()-start_time, "ms")
}
