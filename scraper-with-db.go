package main

import (
	"database/sql"
	"flag"
	"fmt"
	"log"
	"os"
	"time"

	_ "github.com/mattn/go-sqlite3"
)

const (
	dateFormat     = "2006-01-02"
	defaultWorkers = 4
	dbPath         = "scraped_articles.db"
)

var (
	startDate   = flag.String("start", "", "Start date in YYYY-MM-DD format (optional, defaults to last scrape time)")
	endDate     = flag.String("end", "", "End date in YYYY-MM-DD format (optional, defaults to now)")
	parallelism = flag.Int("parallelism", defaultWorkers, "Number of parallel workers")
	help        = flag.Bool("help", false, "Show help message")
)

func initDB() (*sql.DB, error) {
	db, err := sql.Open("sqlite3", dbPath)
	if err != nil {
		return nil, fmt.Errorf("failed to open database: %w", err)
	}

	// Create table if not exists
	_, err = db.Exec(`
		CREATE TABLE IF NOT EXISTS articles (
			id INTEGER PRIMARY KEY AUTOINCREMENT,
			url TEXT UNIQUE NOT NULL,
			title TEXT,
			content TEXT,
			published_date DATETIME,
			scraped_at DATETIME DEFAULT CURRENT_TIMESTAMP,
			category TEXT
		)
	`)
	if err != nil {
		return nil, fmt.Errorf("failed to create table: %w", err)
	}

	return db, nil
}

func saveArticle(db *sql.DB, article ArticleResult) error {
	_, err := db.Exec(`
		INSERT OR REPLACE INTO articles (url, title, content, published_date, scraped_at, category)
		VALUES (?, ?, ?, ?, ?, ?)
	`,
		article.URL,
		article.Title,
		article.Content,
		article.PublishedDate.Format("2006-01-02 15:04:05"),
		article.ScrapedAt.Format("2006-01-02 15:04:05"),
		article.Category,
	)
	return err
}

func main() {
	flag.Parse()

	if *help {
		flag.Usage()
		os.Exit(0)
	}

	// Parse dates
	var start, end time.Time
	var err error

	if *startDate == "" {
		start = GetLastScrapedTime()
		log.Printf("Using auto-detected start date from last scrape: %s\n", start.Format(dateFormat))
	} else {
		start, err = time.Parse(dateFormat, *startDate)
		if err != nil {
			log.Fatalf("Invalid start date: %v", err)
		}
	}

	if *endDate == "" {
		end = time.Now()
		log.Printf("Using current time as end date: %s\n", end.Format(dateFormat))
	} else {
		end, err = time.Parse(dateFormat, *endDate)
		if err != nil {
			log.Fatalf("Invalid end date: %v", err)
		}
	}

	// Initialize database
	db, err := initDB()
	if err != nil {
		log.Fatalf("Failed to initialize database: %v", err)
	}
	defer db.Close()

	log.Printf("Database initialized: %s\n", dbPath)

	// Create scraper
	scraper := CreateSraper(Options{
		StartTime:   start,
		EndTime:     end,
		Parallelism: *parallelism,
	})

	// Get results channel
	results := scraper.GetResults()

	// Consume results and save to database
	articleCount := 0
	go func() {
		for article := range results {
			if err := saveArticle(db, article); err != nil {
				log.Printf("Failed to save article %s: %v\n", article.URL, err)
			} else {
				articleCount++
				if articleCount%10 == 0 {
					log.Printf("Saved %d articles to database...\n", articleCount)
				}
			}
		}
		log.Printf("Total articles saved: %d\n", articleCount)
	}()

	// Start scraping
	log.Println("Starting scrape operation...")
	scraper.Scrape()

	// Save timestamp
	if err := SaveLastScrapedTime(end); err != nil {
		log.Printf("Warning: Failed to save last scraped timestamp: %v\n", err)
	}

	log.Println("Scrape operation completed successfully")
}
