package main

import (
	"fmt"
	"time"
)

func currentTimeMilisecond() int64 {
	return time.Time.UnixNano(time.Now()) / 1000 / 1000
}

func main() {
	fmt.Println(currentTimeMilisecond())
}
