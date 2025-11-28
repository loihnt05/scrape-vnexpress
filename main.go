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
	startDate   = flag.String("start", "", "Start date in YYYY-MM-DD format (required)")
	endDate     = flag.String("end", "", "End date in YYYY-MM-DD format (required)")
	parallelism = flag.Int("parallelism", defaultWorkers, "Number of parallel workers")
	labelFlag   = flag.String("label", "undefined", "Label for scraped articles: trusted|untrusted|undefined")
	help        = flag.Bool("help", false, "Show help message")
)

func init() {
	flag.Usage = func() {
		fmt.Fprintf(os.Stderr, "VNExpress Scraper\n\n")
		fmt.Fprintf(os.Stderr, "Usage: %s [options]\n\n", os.Args[0])
		fmt.Fprintf(os.Stderr, "Options:\n")
		flag.PrintDefaults()
		fmt.Fprintf(os.Stderr, "\nExample:\n")
		fmt.Fprintf(os.Stderr, "  %s -start 2020-01-01 -end 2020-02-01 -parallelism 4\n", os.Args[0])
	}
}

func parseDate(dateStr string, fieldName string) (time.Time, error) {
	if dateStr == "" {
		return time.Time{}, fmt.Errorf("%s is required", fieldName)
	}

	parsedDate, err := time.Parse(dateFormat, dateStr)
	if err != nil {
		return time.Time{}, fmt.Errorf("invalid %s format: %s (expected YYYY-MM-DD)", fieldName, dateStr)
	}

	return parsedDate, nil
}

func validateOptions(start, end time.Time, workers int) error {
	if start.IsZero() || end.IsZero() {
		return fmt.Errorf("start and end dates are required")
	}

	if end.Before(start) {
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

	// Validate options
	if err := validateOptions(start, end, *parallelism); err != nil {
		log.Printf("Error: %v\n\n", err)
		flag.Usage()
		os.Exit(1)
	}

	// Validate label flag
	allowed := map[string]bool{"trusted": true, "untrusted": true, "undefined": true}
	if _, ok := allowed[*labelFlag]; !ok {
		log.Printf("Error: invalid label '%s' (allowed: trusted, untrusted, undefined)\n\n", *labelFlag)
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
		StartTime:    start,
		EndTime:      end,
		Parallelism:  *parallelism,
		DefaultLabel: *labelFlag,
	})

	log.Println("Starting scrape operation...")
	scraper.Scrape()
	log.Println("Scrape operation completed successfully")
}
