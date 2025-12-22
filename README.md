# VNExpress Scraper

A Go-based web scraper for VNExpress articles designed for Kafka integration with automatic incremental scraping.

## Features

✅ **Auto-Scraping**: Automatically tracks last scrape time and scrapes incrementally  
✅ **Kafka-Ready**: Outputs data in structured format for Kafka producer  
✅ **No Database Required**: Removed PostgreSQL dependency  
✅ **Rate Limiting**: Built-in rate limiting and random delays  
✅ **Parallel Processing**: Configurable parallel workers  
✅ **Category Support**: Scrapes all major VNExpress categories  

## Quick Start

### 1. Build the Application
```bash
go build -o scrape-vnexpress
```

### 2. Run the Scraper (Auto Mode)
```bash
# First run: scrapes last 7 days
./scrape-vnexpress

# Subsequent runs: scrapes from last run to now
./scrape-vnexpress
```

### 3. Manual Date Range (Optional)
```bash
# Scrape specific date range
./scrape-vnexpress -start 2024-01-01 -end 2024-01-31 -parallelism 4

# Scrape from specific date to now
./scrape-vnexpress -start 2024-12-01
```

## How Auto-Scraping Works

The scraper tracks the last scrape time in `.last_scraped_at` file:

1. **First Run**: Scrapes from 7 days ago to now
2. **Subsequent Runs**: Scrapes from last saved time to now  
3. **After Each Run**: Updates timestamp to current time

See [AUTO_SCRAPE_GUIDE.md](AUTO_SCRAPE_GUIDE.md) for detailed documentation.

## Configuration

### Environment Variables

- `PROXY_URL`: Optional HTTP/HTTPS proxy (e.g., `http://proxy:8080`)

### Command Line Options

```bash
./scrape-vnexpress [options]

Options:
  -start string
        Start date in YYYY-MM-DD format (optional, defaults to last scrape time)
  -end string
        End date in YYYY-MM-DD format (optional, defaults to now)
  -parallelism int
        Number of parallel workers (default 4)
  -help
        Show help message
```

## Output Format

Each scraped article is emitted as an `ArticleResult` with this structure:

```json
{
  "url": "https://vnexpress.net/article-url",
  "title": "Article Title",
  "content": "Full article content...",
  "published_date": "2024-01-01 10:30:00",
  "scraped_at": "2024-12-22 15:45:00",
  "category": "Thời sự"
}
```

See [KAFKA_MIGRATION.md](KAFKA_MIGRATION.md) for Kafka integration details.

## Supported Categories

- Thời sự (Politics)
- Thế giới (World)
- Kinh doanh (Business)
- Bất động sản (Real Estate)
- Giải trí (Entertainment)
- Thể thao (Sports)
- Pháp luật (Law)
- Giáo dục (Education)
- Sức khỏe (Health)
- Đời sống (Lifestyle)
- Du lịch (Travel)
- Khoa học công nghệ (Technology)
- Xe (Automotive)
- Ý kiến (Opinion)
- And more...

## Documentation

- [AUTO_SCRAPE_GUIDE.md](AUTO_SCRAPE_GUIDE.md) - Auto-scraping and scheduling guide
- [KAFKA_MIGRATION.md](KAFKA_MIGRATION.md) - Kafka integration and migration details
- [kafka_example.go](kafka_example.go) - Example code for Kafka integration

## Architecture

```
Input: Date Range (auto or manual)
  ↓
Link Generator (by category & date)
  ↓
Link Queue (buffered channel)
  ↓
Link Consumers (parallel workers)
  ↓
Article Scraper (colly)
  ↓
Results Channel → Your Kafka Producer
  ↓
.last_scraped_at (timestamp tracking)
```

### Stop Database
```bash
sudo docker compose down
```

### Stop and Remove Data
```bash
sudo docker compose down -v
```

## Features

- Scrapes articles from VNExpress by date range
- Parallel processing support
- PostgreSQL storage with automatic deduplication
- Caching of HTTP requests
- Rate limiting to respect server resources


## Use proxy 
```bash
# Database Configuration
DB_HOST=localhost
DB_PORT=5432
DB_USER=your-user
DB_PASSWORD=your-password
DB_NAME=your-name
DB_SSLMODE=disable

PROXY_URL=your-url-proxy
```