package main

import (
	"fmt"
	"time"
)

func main() {
	fmt.Println("helo")
	scraper := CreateSraper(Options{
		StartTime: time.Now().AddDate(-5, 0, 0),
		EndTime:   time.Now(),
	})
	scraper.Scrape()
}
