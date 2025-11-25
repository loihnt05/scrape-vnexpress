package main

import (
	"context"
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
	"golang.org/x/time/rate"
)

var categories = map[int]string{
	1001005: "Thời sự",

	1001002: "Thế giới",
	1003159: "Kinh doanh",
	1005628: "Bất động sản",
	1002691: "Giải trí",
	1002565: "Thể thao",
	1001007: "Pháp luật",
	1003497: "Giáo dục",
	1003750: "Sức khỏe",
	1002966: "Đời sống",
	1003231: "Du lịch",
	1006219: "Khoa học công nghệ",
	1001006: "Xe",
	1001012: "Ý kiến",
	1001014: "Tâm sự",
	1001011: "Cười",
	1004565: "Tuyến đầu chống dịch",
}

func extractLinksFromTitleNews(htmlContent string) []string {
	var links []string

	// 1. Create a new tokenizer over the HTML content
	doc, err := html.Parse(strings.NewReader(htmlContent))
	if err != nil {
		log.Printf("Error parsing HTML: %v\n", err)
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
	links         chan string
	client        *retryablehttp.Client
	newsCollector *colly.Collector
	options       Options
	wg            *sync.WaitGroup
	db            *sql.DB
}

type Options struct {
	StartTime   time.Time
	EndTime     time.Time
	Parallelism int
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
	if options.Parallelism == 0 {
		options.Parallelism = 4
	}
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
	newsCollector.Limit(&colly.LimitRule{DomainGlob: "*", Parallelism: options.Parallelism})

	// rp, err := proxy.RoundRobinProxySwitcher("socks4://147.45.170.65:1080")
	// if err != nil {
	// 	log.Fatal(err)
	// }
	// newsCollector.SetProxyFunc(rp)

	scraper := &Scraper{
		client:        retryablehttp.NewClient(),
		options:       options,
		newsCollector: newsCollector,
		writes:        make(chan WriteRequest, 10000),
		links:         make(chan string, 10000),
		wg:            &sync.WaitGroup{},
		db:            db,
	}
	scraper.Setup()
	return scraper
}

func (s *Scraper) ProcessWrite() {
	upsertSQL := `
		INSERT INTO articles(url, title, description, article_html, category_id, category_name)
		VALUES(?, ?, ?, ?, ?, ?)
		ON CONFLICT (url) DO UPDATE SET
			title = EXCLUDED.title,
			description = EXCLUDED.description,
			article_html = EXCLUDED.article_html,
			category_id = EXCLUDED.category_id,
			category_name = EXCLUDED.category_name;
	`

	for item := range s.writes {
		// Log start of processing
		log.Printf("Processing Write/Update for: **%s**\n", item.Title)

		// Execute the SQL upsert
		_, err := s.db.Exec(upsertSQL, item.URL, item.Title, item.Description, item.ArticleHtml, item.CategoryId, item.CategoryName)
		if err != nil {
			log.Printf("❌ Error writing/updating article '%s' to database: %v", item.Title, err)
		} else {
			log.Printf("Successfully wrote/updated article: %s\n", item.Title)
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

func (s *Scraper) ConsumeLinks() {
	for link := range s.links {
		err := s.newsCollector.Visit(link)
		if err != nil {
			log.Printf("Error visiting %s: %v\n", link, err)
		}
	}
}

func (s *Scraper) Scrape() {
	s.wg.Go(s.ProcessWrite)

	// Start link consumers
	numConsumers := 3
	for i := 0; i < numConsumers; i++ {
		s.wg.Go(s.ConsumeLinks)
	}

	// Start iterating from the initial StartTime
	currentTime := s.options.StartTime

	limiter := rate.NewLimiter(rate.Every(1000*time.Millisecond), 5)

	linkGeneratorWg := &sync.WaitGroup{}
	for currentTime.Before(s.options.EndTime) {
		dayStart := time.Date(currentTime.Year(), currentTime.Month(), currentTime.Day(), 0, 0, 0, 0, currentTime.Location())
		nextDayStart := dayStart.AddDate(0, 0, 1)
		dayEnd := nextDayStart.Add(-time.Nanosecond)

		scrapeEnd := dayEnd
		if s.options.EndTime.Before(dayEnd) {
			scrapeEnd = s.options.EndTime
		}

		for categoryId := range categories {
			linkGeneratorWg.Go(func() {
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
						s.links <- link
						continue
					}
					if exists {
						log.Printf("Skipping already-saved link: %s\n", link)
						continue
					}
					s.links <- link
				}
			})
		}

		currentTime = nextDayStart
	}

	linkGeneratorWg.Wait()
	close(s.links)

	s.newsCollector.Wait()

	close(s.writes)

	s.wg.Wait()
}
