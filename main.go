package main

import (
	"flag"
	"fmt"
	"log"
	"os"
	"time"
)

const (
	dateFormat     = "2006-01-02"
	defaultWorkers = 4
)

var (
	startDate   = flag.String("start", "", "Start date in YYYY-MM-DD format (optional, defaults to last scrape time)")
	endDate     = flag.String("end", "", "End date in YYYY-MM-DD format (optional, defaults to now)")
	parallelism = flag.Int("parallelism", defaultWorkers, "Number of parallel workers")
	help        = flag.Bool("help", false, "Show help message")
)

func init() {
	flag.Usage = func() {
		fmt.Fprintf(os.Stderr, "VNExpress Scraper\n\n")
		fmt.Fprintf(os.Stderr, "Usage: %s [options]\n\n", os.Args[0])
		fmt.Fprintf(os.Stderr, "Options:\n")
		flag.PrintDefaults()
		fmt.Fprintf(os.Stderr, "\nExamples:\n")
		fmt.Fprintf(os.Stderr, "  # Scrape from last saved timestamp to now (automatic)\n")
		fmt.Fprintf(os.Stderr, "  %s\n\n", os.Args[0])
		fmt.Fprintf(os.Stderr, "  # Scrape specific date range\n")
		fmt.Fprintf(os.Stderr, "  %s -start 2020-01-01 -end 2020-02-01 -parallelism 4\n\n", os.Args[0])
		fmt.Fprintf(os.Stderr, "  # Scrape from specific date to now\n")
		fmt.Fprintf(os.Stderr, "  %s -start 2020-01-01\n", os.Args[0])
	}
}

func parseDate(dateStr string, fieldName string) (time.Time, error) {
	if dateStr == "" {
		// Empty date is now valid, will be handled by caller
		return time.Time{}, nil
	}

	parsedDate, err := time.Parse(dateFormat, dateStr)
	if err != nil {
		return time.Time{}, fmt.Errorf("invalid %s format: %s (expected YYYY-MM-DD)", fieldName, dateStr)
	}

	return parsedDate, nil
}

func validateOptions(start, end time.Time, workers int) error {
	if !start.IsZero() && !end.IsZero() && end.Before(start) {
		return fmt.Errorf("end date must be after start date")
	}

	if workers < 1 {
		return fmt.Errorf("parallelism must be at least 1")
	}

	if workers > 100 {
		return fmt.Errorf("parallelism cannot exceed 100")
	}

	return nil
}

func main() {
	flag.Parse()

	if *help {
		flag.Usage()
		os.Exit(0)
	}

	// Parse dates
	start, err := parseDate(*startDate, "start date")
	if err != nil {
		log.Printf("Error: %v\n\n", err)
		flag.Usage()
		os.Exit(1)
	}

	end, err := parseDate(*endDate, "end date")
	if err != nil {
		log.Printf("Error: %v\n\n", err)
		flag.Usage()
		os.Exit(1)
	}

	// Auto-detect date range if not provided
	if start.IsZero() {
		start = GetLastScrapedTime()
		log.Printf("Using auto-detected start date from last scrape: %s\n", start.Format(dateFormat))
	}

	if end.IsZero() {
		end = time.Now()
		log.Printf("Using current time as end date: %s\n", end.Format(dateFormat))
	}

	// Validate options
	if err := validateOptions(start, end, *parallelism); err != nil {
		log.Printf("Error: %v\n\n", err)
		flag.Usage()
		os.Exit(1)
	}

	// Log configuration
	log.Printf("Starting VNExpress scraper with configuration:")
	log.Printf("  Start Date: %s", start.Format(dateFormat))
	log.Printf("  End Date: %s", end.Format(dateFormat))
	log.Printf("  Parallelism: %d workers", *parallelism)
	log.Printf("  Duration: %d days", int(end.Sub(start).Hours()/24))

	// Create and run scraper
	scraper := CreateSraper(Options{
		StartTime:   start,
		EndTime:     end,
		Parallelism: *parallelism,
	})

	log.Println("Starting scrape operation...")
	scraper.Scrape()
	
	// Save the end time as the last scraped timestamp
	if err := SaveLastScrapedTime(end); err != nil {
		log.Printf("Warning: Failed to save last scraped timestamp: %v\n", err)
	}
	
	log.Println("Scrape operation completed successfully")
}
