package main

import "time"

func main() {
	scraper := CreateSraper(Options{
		StartTime: time.Now().AddDate(0, 0, -100),
		EndTime:   time.Now(),
	})
	scraper.Scrape()
}
