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
	CREATE TABLE IF NOT EXISTS articles (
		"title" TEXT NOT NULL UNIQUE, "description" TEXT,
		"article_html" TEXT,
		"scraped_at" DATETIME DEFAULT CURRENT_TIMESTAMP
	);`

	_, err = db.Exec(createTableSQL)
	if err != nil {
		db.Close()
		return nil, fmt.Errorf("error creating table: %w", err)
	}

	fmt.Printf("Database initialized at %s\n", dbPath)
	return db, nil
}
