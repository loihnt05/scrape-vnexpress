package main

import (
	"fmt"
	"time"
)

func main() {
	fmt.Println("helo")
	scraper := CreateSraper(Options{
		StartTime: time.Date(2020, time.January, 1, 0, 0, 0, 0, time.UTC),
		EndTime:   time.Now(),
	})
	scraper.Scrape()
}
