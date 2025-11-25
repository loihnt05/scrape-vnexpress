package main

import (
	"database/sql"
	"fmt"
	"io"
	"log"
	"strings"
	"sync"
	"time"

	"github.com/gocolly/colly"
	"github.com/gocolly/colly/debug"
	"github.com/gocolly/colly/extensions"
	"github.com/hashicorp/go-retryablehttp"
	_ "github.com/mattn/go-sqlite3"
	"golang.org/x/net/html"
)

func extractLinksFromTitleNews(htmlContent string) []string {
	var links []string

	// 1. Create a new tokenizer over the HTML content
	doc, err := html.Parse(strings.NewReader(htmlContent))
	if err != nil {
		fmt.Printf("Error parsing HTML: %v\n", err)
		return links // Return empty list on parse error
	}

	// 2. Define the recursive function to traverse the nodes
	var f func(*html.Node)
	var inTitleNews bool = false // Flag to track if we are currently inside a "title-news" element

	f = func(n *html.Node) {
		// Save the current state of the flag before changing it
		prevInTitleNews := inTitleNews

		// Check if the current node is an element with class "title-news"
		if n.Type == html.ElementNode {
			for _, a := range n.Attr {
				if a.Key == "class" && strings.Contains(a.Val, "title-news") {
					// We are entering a "title-news" block
					inTitleNews = true
					break
				}
			}
		}

		// Check if the current node is an <a> tag and we are inside a "title-news" block
		if inTitleNews && n.Type == html.ElementNode && n.Data == "a" {
			for _, a := range n.Attr {
				if a.Key == "href" {
					// Add the link to our list
					links = append(links, a.Val)
				}
			}
		}

		// Traverse the children nodes recursively
		for c := n.FirstChild; c != nil; c = c.NextSibling {
			f(c)
		}

		// Restore the flag's state as we exit the current node's recursion
		inTitleNews = prevInTitleNews
	}

	// 3. Start the traversal from the root document node
	f(doc)

	return links
}

type WriteRequest struct {
	ArticleHtml string
	Title       string
	Description string
}

type Scraper struct {
	writes        chan WriteRequest
	client        *retryablehttp.Client
	newsCollector *colly.Collector
	options       Options
	wg            *sync.WaitGroup
	db            *sql.DB
}

type Options struct {
	StartTime time.Time
	EndTime   time.Time
}

// categoryId 1003159
func getBatchUrl(categoryId int64, startTime time.Time, endTime time.Time) string {
	return fmt.Sprintf("https://vnexpress.net/category/day/cateid/%d/fromdate/%d/todate/%d", categoryId, startTime.Unix(), endTime.Unix())
}

func (s *Scraper) GetPage(startTime time.Time, endTime time.Time, categoryId int64) (string, error) {
	url := getBatchUrl(categoryId, startTime, endTime)
	res, err := s.client.Get(url)
	if err != nil {
		return "", err
	}
	body, err := io.ReadAll(res.Body)
	if err != nil {
		return "", err
	}
	return string(body), nil
}

func (s *Scraper) Setup() {
	s.newsCollector.OnHTML("div.container.detail-new", func(e *colly.HTMLElement) {
		// date := e.ChildText("span.date")
		title := e.ChildText("h1.title-detail")
		description := e.ChildText("p.description")
		e.ForEach("article.fck_detail ", func(i int, e *colly.HTMLElement) {
			html, err := e.DOM.Html()
			if err != nil {
				panic(err)
			}
			request := WriteRequest{
				ArticleHtml: html,
				Title:       title,
				Description: description,
			}

			s.writes <- request

		})

	})
	extensions.RandomUserAgent(s.newsCollector)
	extensions.Referer(s.newsCollector)
}

func CreateSraper(options Options) *Scraper {
	db, err := setupDatabase(dbPath)
	if err != nil {
		panic(err)
	}
	newsCollector := colly.NewCollector(
		colly.CacheDir("./cache"),
		colly.AllowedDomains("vnexpress.net"),
		colly.Async(true),
		colly.Debugger(&debug.LogDebugger{}),
	)
	scraper := &Scraper{
		client:        retryablehttp.NewClient(),
		options:       options,
		newsCollector: newsCollector,
		writes:        make(chan WriteRequest, 10000),
		wg:            &sync.WaitGroup{},
		db:            db,
	}
	scraper.Setup()
	return scraper
}

func (s *Scraper) ProcessWrite() {
	upsertSQL := `
		INSERT INTO articles(title, description, article_html)
		VALUES(?, ?, ?)
		ON CONFLICT (title) DO UPDATE SET
			description = EXCLUDED.description,
			article_html = EXCLUDED.article_html;
	`

	for item := range s.writes {
		// Log start of processing
		fmt.Printf("📝 Processing Write/Update for: **%s**\n", item.Title)

		// Execute the SQL upsert
		_, err := s.db.Exec(upsertSQL, item.Title, item.Description, item.ArticleHtml)
		if err != nil {
			log.Printf("❌ Error writing/updating article '%s' to database: %v", item.Title, err)
		} else {
			fmt.Printf("✅ Successfully wrote/updated article: %s\n", item.Title)
		}
	}
	// Log that the writer is done
	fmt.Println("--- Write process finished. ---")
}

func (s *Scraper) Scrape() {
	s.wg.Go(s.ProcessWrite)

	// Start iterating from the initial StartTime
	currentTime := s.options.StartTime

	for currentTime.Before(s.options.EndTime) {
		dayStart := time.Date(currentTime.Year(), currentTime.Month(), currentTime.Day(), 0, 0, 0, 0, currentTime.Location())
		nextDayStart := dayStart.AddDate(0, 0, 1)
		dayEnd := nextDayStart.Add(-time.Nanosecond)

		scrapeEnd := dayEnd
		if s.options.EndTime.Before(dayEnd) {
			scrapeEnd = s.options.EndTime
		}

		fmt.Printf("Scraping from %v to %v\n", dayStart, scrapeEnd)

		result, err := s.GetPage(dayStart, scrapeEnd)
		if err != nil {
			panic(err)
		}

		links := extractLinksFromTitleNews(result)

		for _, link := range links {
			s.newsCollector.Visit(link)
		}

		currentTime = nextDayStart
	}

	s.newsCollector.Wait()

	close(s.writes)

	s.wg.Wait()
}
