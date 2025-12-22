package main

import (
	"fmt"
	"io/ioutil"
	"log"
	"os"
	"time"
)

const (
	timestampFile   = ".last_scraped_at"
	timestampFormat = time.RFC3339
)

// GetLastScrapedTime reads the last scrape timestamp from file
// Returns the timestamp or a default time (7 days ago) if file doesn't exist
func GetLastScrapedTime() time.Time {
	data, err := ioutil.ReadFile(timestampFile)
	if err != nil {
		if os.IsNotExist(err) {
			// File doesn't exist, return default (7 days ago)
			defaultTime := time.Now().AddDate(0, 0, -7)
			log.Printf("No last scrape timestamp found. Starting from default: %s\n", defaultTime.Format(timestampFormat))
			return defaultTime
		}
		log.Printf("Error reading timestamp file: %v. Using default (7 days ago)\n", err)
		return time.Now().AddDate(0, 0, -7)
	}

	timestamp, err := time.Parse(timestampFormat, string(data))
	if err != nil {
		log.Printf("Error parsing timestamp: %v. Using default (7 days ago)\n", err)
		return time.Now().AddDate(0, 0, -7)
	}

	log.Printf("Last scraped at: %s\n", timestamp.Format(timestampFormat))
	return timestamp
}

// SaveLastScrapedTime writes the current time as the last scrape timestamp
func SaveLastScrapedTime(t time.Time) error {
	data := []byte(t.Format(timestampFormat))
	err := ioutil.WriteFile(timestampFile, data, 0644)
	if err != nil {
		return fmt.Errorf("error saving timestamp: %w", err)
	}
	log.Printf("Saved last scraped timestamp: %s\n", t.Format(timestampFormat))
	return nil
}
