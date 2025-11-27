# VNExpress Scraper

A Go-based web scraper for VNExpress articles with PostgreSQL storage.

## Setup

### 1. Start PostgreSQL
```bash
sudo docker compose up -d
```

### 2. Check Database Status
```bash
sudo docker compose ps
```

### 3. Build the Application
```bash
go build
```

### 4. Run the Scraper
```bash
./scrape-vnexpress -start 2025-11-24 -end 2025-11-25 -parallelism 2
```

## Database Configuration

The application uses environment variables for database configuration (defaults shown):

- `DB_HOST=localhost`
- `DB_PORT=5432`
- `DB_USER=vnexpress`
- `DB_PASSWORD=vnexpress123`
- `DB_NAME=vnexpress_scraper`
- `DB_SSLMODE=disable`

See `.env.example` for reference.

## Database Management

### Connect to PostgreSQL
```bash
sudo docker exec -it vnexpress-postgres psql -U vnexpress -d vnexpress_scraper
```

### View Tables
```sql
\dt
```

### Query Articles
```sql
SELECT url, title, published_date FROM articles LIMIT 10;
```

### Count Articles
```sql
SELECT COUNT(*) FROM articles;
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