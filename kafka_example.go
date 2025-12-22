package main

import (
	"encoding/json"
	"log"
	"time"
)

// Example: How to use the refactored scraper with Kafka producer
// This demonstrates how to consume scraped articles from the results channel

// KafkaProducerExample shows how to integrate with a Kafka producer
func KafkaProducerExample() {
	// Create scraper with your desired date range
	scraper := CreateSraper(Options{
		StartTime:   time.Date(2024, 1, 1, 0, 0, 0, 0, time.UTC),
		EndTime:     time.Date(2024, 1, 2, 0, 0, 0, 0, time.UTC),
		Parallelism: 4,
	})

	// Get the results channel
	results := scraper.GetResults()

	// Start a goroutine to consume results and send to Kafka
	go func() {
		for article := range results {
			// Convert to JSON format
			jsonData := article.ToJSON()
			
			// Serialize to JSON bytes
			messageBytes, err := json.Marshal(jsonData)
			if err != nil {
				log.Printf("Error marshaling article to JSON: %v\n", err)
				continue
			}

			// TODO: Send to Kafka producer
			// Example (pseudo-code):
			// kafkaProducer.Send("vnexpress-articles", messageBytes)
			
			log.Printf("Ready to send to Kafka: %s\n", string(messageBytes))
		}
		
		log.Println("All articles processed")
	}()

	// Start scraping (this will block until complete)
	scraper.Scrape()
	
	log.Println("Scraping completed")
}

// Example output format that matches your required schema:
// {
//   "url": "https://vnexpress.net/article-url",
//   "title": "Article Title",
//   "content": "Full article content...",
//   "published_date": "2024-01-01 10:30:00",
//   "scraped_at": "2024-12-22 15:45:00",
//   "category": "Thời sự"
// }
