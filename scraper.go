package main

import (
	"bufio"
	"context"
	"database/sql"
	"fmt"
	"io"
	"log"
	"net/http"
	"strings"
	"sync"
	"time"

	"github.com/gocolly/colly"
	"github.com/gocolly/colly/debug"
	"github.com/gocolly/colly/extensions"
	"github.com/gocolly/colly/proxy"
	"github.com/gocolly/colly/queue"
	"github.com/hashicorp/go-retryablehttp"
	_ "github.com/mattn/go-sqlite3"
	"golang.org/x/net/html"
	"golang.org/x/time/rate"
)

// getProxiesAsStrings fetches content from the given URL and returns it as a slice of strings,
// where each element is a non-empty line from the response body.
// It will panic immediately if any error occurs during fetching or reading.
func getProxiesAsStrings() []string {
	url := "https://api.proxyscrape.com/v4/free-proxy-list/get?request=display_proxies&proxy_format=protocolipport&format=text"
	// 1. Perform the HTTP GET request.
	resp, err := http.Get(url)
	if err != nil {
		// Panic immediately on request error
		panic(fmt.Errorf("error performing request to %s: %w", url, err))
	}
	// Ensure the response body is closed when the function returns.
	defer resp.Body.Close()

	// 2. Check for a successful status code.
	if resp.StatusCode != http.StatusOK {
		// Panic immediately on non-OK status
		panic(fmt.Errorf("received non-OK HTTP status code: %d %s", resp.StatusCode, resp.Status))
	}

	// 3. Initialize a slice to hold the lines (proxies).
	var lines []string

	// 4. Use bufio.NewScanner to read the response body line by line.
	// This is efficient for reading stream data like HTTP response bodies.
	scanner := bufio.NewScanner(resp.Body)

	for scanner.Scan() {
		line := strings.TrimSpace(scanner.Text())
		// Only add non-empty lines to the result slice.
		if line != "" {
			lines = append(lines, line)
		}
	}

	// 5. Check for any errors encountered during scanning/reading.
	if err := scanner.Err(); err != nil && err != io.EOF {
		// Panic immediately on scanner/read error
		panic(fmt.Errorf("error reading response body: %w", err))
	}

	return lines
}

var categories = map[int]string{
	1001005: "Thời sự",
	// 1003450: "Góc nhìn",
	// 1001002: "Thế giới",
	// 1003159: "Kinh doanh",
	// 1005628: "Bất động sản",
	// 1002691: "Giải trí",
	// 1002565: "Thể thao",
	// 1001007: "Pháp luật",
	// 1003497: "Giáo dục",
	// 1003750: "Sức khỏe",
	// 1002966: "Đời sống",
	// 1003231: "Du lịch",
	// 1006219: "Khoa học công nghệ",
	// 1001006: "Xe",
	// 1001012: "Ý kiến",
	// 1001014: "Tâm sự",
	// 1001011: "Cười",
	// 1004565: "Tuyến đầu chống dịch",
}

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
	ArticleHtml  string
	Title        string
	Description  string
	URL          string
	CategoryId   int64
	CategoryName string
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
				URL:         e.Request.URL.String(),
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
		colly.Async(false),
		colly.Debugger(&debug.LogDebugger{}),
	)

	rp, err := proxy.RoundRobinProxySwitcher(getProxiesAsStrings()...)
	if err != nil {
		log.Fatal(err)
	}
	newsCollector.SetProxyFunc(rp)

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
		INSERT INTO articles(url, title, description, article_html)
		VALUES(?, ?, ?, ?)
		ON CONFLICT (url) DO UPDATE SET
			title = EXCLUDED.title,
			description = EXCLUDED.description,
			article_html = EXCLUDED.article_html;
	`

	for item := range s.writes {
		// Log start of processing
		fmt.Printf("Processing Write/Update for: **%s**\n", item.Title)

		// Execute the SQL upsert
		_, err := s.db.Exec(upsertSQL, item.URL, item.Title, item.Description, item.ArticleHtml)
		if err != nil {
			log.Printf("❌ Error writing/updating article '%s' to database: %v", item.Title, err)
		} else {
			fmt.Printf("Successfully wrote/updated article: %s\n", item.Title)
		}
	}
}

// LinkExists checks whether a given URL already exists in the articles table.
func (s *Scraper) LinkExists(url string) (bool, error) {
	var v int
	err := s.db.QueryRow("SELECT 1 FROM articles WHERE url = ? LIMIT 1", url).Scan(&v)
	if err == sql.ErrNoRows {
		return false, nil
	}
	if err != nil {
		return false, err
	}
	return true, nil
}

func (s *Scraper) Scrape() {
	s.wg.Go(s.ProcessWrite)

	q, err := queue.New(
		1, // Number of consumer threads
		&queue.InMemoryQueueStorage{MaxSize: 10000}, // Use default queue storage
	)

	if err != nil {
		panic(err)
	}

	// Start iterating from the initial StartTime
	currentTime := s.options.StartTime

	limiter := rate.NewLimiter(rate.Every(1000*time.Millisecond), 5)

	for currentTime.Before(s.options.EndTime) {
		dayStart := time.Date(currentTime.Year(), currentTime.Month(), currentTime.Day(), 0, 0, 0, 0, currentTime.Location())
		nextDayStart := dayStart.AddDate(0, 0, 1)
		dayEnd := nextDayStart.Add(-time.Nanosecond)

		scrapeEnd := dayEnd
		if s.options.EndTime.Before(dayEnd) {
			scrapeEnd = s.options.EndTime
		}

		for categoryId := range categories {
			s.wg.Go(func() {
				err := limiter.Wait(context.Background())
				if err != nil {
					panic(err)
				}
				result, err := s.GetPage(dayStart, scrapeEnd, int64(categoryId))
				if err != nil {
					panic(err)
				}

				links := extractLinksFromTitleNews(result)

				for _, link := range links {
					// Check DB to avoid refetching links we've already saved
					exists, err := s.LinkExists(link)
					if err != nil {
						log.Printf("error checking link existence for %s: %v — will queue it", link, err)
						q.AddURL(link)
						continue
					}
					if exists {
						fmt.Printf("Skipping already-saved link: %s\n", link)
						continue
					}
					q.AddURL(link)
				}
			})
		}

		currentTime = nextDayStart
	}

	q.Run(s.newsCollector)

	close(s.writes)

	s.wg.Wait()
}
