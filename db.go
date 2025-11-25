package main

import (
	"database/sql"
	"fmt"
)

const dbPath = "./scraped_articles.db"

func setupDatabase(dbPath string) (*sql.DB, error) {
	// 1. Open the database file
	db, err := sql.Open("sqlite3", dbPath)
	if err != nil {
		return nil, fmt.Errorf("error opening database: %w", err)
	}

	// 2. Create the articles table if it doesn't exist
	createTableSQL := `
	PRAGMA journal_mode=WAL;
	CREATE TABLE IF NOT EXISTS articles (
		"url" TEXT NOT NULL UNIQUE,
		"title" TEXT,
		"description" TEXT,
		"article_html" TEXT,
		"scraped_at" DATETIME DEFAULT CURRENT_TIMESTAMP,
		"published_date" DATETIME
	);`

	_, err = db.Exec(createTableSQL)
	if err != nil {
		db.Close()
		return nil, fmt.Errorf("error creating table: %w", err)
	}

	// Ensure migrations: if `url` column was not present in an older DB, add it.
	// Check table info for `articles` and add the `url` column if missing.
	rows, err := db.Query(`PRAGMA table_info(articles);`)
	if err == nil {
		defer rows.Close()
		hasURL := false
		for rows.Next() {
			var cid int
			var name string
			var ctype string
			var notnull int
			var dflt_value interface{}
			var pk int
			if err := rows.Scan(&cid, &name, &ctype, &notnull, &dflt_value, &pk); err == nil {
				if name == "url" {
					hasURL = true
					break
				}
			}
		}
		if !hasURL {
			_, err := db.Exec(`ALTER TABLE articles ADD COLUMN url TEXT;`)
			if err == nil {
				// Create a unique index on url to mimic the new schema uniqueness
				_, _ = db.Exec(`CREATE UNIQUE INDEX IF NOT EXISTS idx_articles_url ON articles(url);`)
			}
		}
	}

	fmt.Printf("Database initialized at %s\n", dbPath)
	return db, nil
}
