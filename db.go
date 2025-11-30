package main

import (
	"database/sql"
	"fmt"
	"os"

	_ "github.com/lib/pq"
)

func getEnv(key, fallback string) string {
	if value := os.Getenv(key); value != "" {
		return value
	}
	return fallback
}

func getPostgresConnectionString() string {
	host := getEnv("DB_HOST", "localhost")
	port := getEnv("DB_PORT", "5432")
	user := getEnv("DB_USER", "vnexpress")
	password := getEnv("DB_PASSWORD", "vnexpress123")
	dbname := getEnv("DB_NAME", "vnexpress_scraper")
	sslmode := getEnv("DB_SSLMODE", "disable")

	return fmt.Sprintf("host=%s port=%s user=%s password=%s dbname=%s sslmode=%s",
		host, port, user, password, dbname, sslmode)
}

func setupDatabase() (*sql.DB, error) {
	connStr := getPostgresConnectionString()

	// 1. Open the database connection
	db, err := sql.Open("postgres", connStr)
	if err != nil {
		return nil, fmt.Errorf("error opening database: %w", err)
	}

	// 2. Test the connection
	if err := db.Ping(); err != nil {
		db.Close()
		return nil, fmt.Errorf("error connecting to database: %w", err)
	}

	// 3. Create the articles table if it doesn't exist
	createTableSQL := `
	CREATE TABLE IF NOT EXISTS articles (
		id SERIAL PRIMARY KEY,
		url TEXT NOT NULL UNIQUE,
		title TEXT,
		description TEXT,
		content TEXT,
		label TEXT DEFAULT 'undefined',
		scraped_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
		published_date TIMESTAMP,
		category TEXT
	);`

	_, err = db.Exec(createTableSQL)
	if err != nil {
		db.Close()
		return nil, fmt.Errorf("error creating table: %w", err)
	}

	// 4. Add category column if it doesn't exist (for existing databases)
	alterTableSQL := `
	DO $$
	BEGIN
		IF NOT EXISTS (
			SELECT 1 FROM information_schema.columns 
			WHERE table_name='articles' AND column_name='category'
		) THEN
			ALTER TABLE articles ADD COLUMN category TEXT;
		END IF;
	END $$;`
	_, err = db.Exec(alterTableSQL)
	if err != nil {
		db.Close()
		return nil, fmt.Errorf("error adding category column: %w", err)
	}

	// 5. Create index on URL for faster lookups
	createIndexSQL := `CREATE INDEX IF NOT EXISTS idx_articles_url ON articles(url);`
	_, err = db.Exec(createIndexSQL)
	if err != nil {
		db.Close()
		return nil, fmt.Errorf("error creating index: %w", err)
	}

	fmt.Printf("Database initialized: %s@%s:%s/%s\n",
		getEnv("DB_USER", "vnexpress"),
		getEnv("DB_HOST", "localhost"),
		getEnv("DB_PORT", "5432"),
		getEnv("DB_NAME", "vnexpress_scraper"))
	return db, nil
}
