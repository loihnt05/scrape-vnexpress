package main

import (
	"context"
	"crypto/tls"
	"database/sql"
	"fmt"
	"io"
	"log"
	"math/rand"
	"net/http"
	"net/url"
	"strings"
	"sync"
	"time"
	"os"

	"github.com/gocolly/colly"
	"github.com/gocolly/colly/debug"
	"github.com/gocolly/colly/extensions"
	"github.com/hashicorp/go-retryablehttp"
	"golang.org/x/net/html"
	"golang.org/x/time/rate"
)

// extractTextContent extracts clean text from HTML, preserving paragraphs
func extractTextContent(htmlContent string) string {
	doc, err := html.Parse(strings.NewReader(htmlContent))
	if err != nil {
		log.Printf("Error parsing HTML for text extraction: %v\n", err)
		return ""
	}

	var textBuilder strings.Builder
	var f func(*html.Node)

	f = func(n *html.Node) {
		// Skip script and style tags entirely
		if n.Type == html.ElementNode {
			switch n.Data {
			case "script", "style", "noscript":
				return // Don't process these tags or their children
			}
		}

		if n.Type == html.TextNode {
			text := strings.TrimSpace(n.Data)
			if text != "" {
				textBuilder.WriteString(text)
				textBuilder.WriteString(" ")
			}
		}
		// Add newlines after paragraphs and other block elements for better formatting
		if n.Type == html.ElementNode {
			switch n.Data {
			case "p", "div", "br", "h1", "h2", "h3", "h4", "h5", "h6":
				if textBuilder.Len() > 0 && !strings.HasSuffix(textBuilder.String(), "\n") {
					textBuilder.WriteString("\n")
				}
			}
		}

		for c := n.FirstChild; c != nil; c = c.NextSibling {
			f(c)
		}

		// Add newline after block elements
		if n.Type == html.ElementNode {
			switch n.Data {
			case "p", "div", "h1", "h2", "h3", "h4", "h5", "h6":
				if textBuilder.Len() > 0 && !strings.HasSuffix(textBuilder.String(), "\n") {
					textBuilder.WriteString("\n")
				}
			}
		}
	}

	f(doc)

	// Clean up the text: remove multiple spaces and newlines
	text := textBuilder.String()
	lines := strings.Split(text, "\n")
	var cleanedLines []string
	for _, line := range lines {
		line = strings.TrimSpace(line)
		if line != "" {
			cleanedLines = append(cleanedLines, line)
		}
	}

	return strings.Join(cleanedLines, "\n\n")
}

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
	Content       string
	Title         string
	Description   string
	URL           string
	PublishedDate time.Time
	Label         string
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
	DefaultLabel string
}

// categoryId 1003159
func getBatchUrl(categoryId int64, startTime time.Time, endTime time.Time) string {
	return fmt.Sprintf("https://vnexpress.net/category/day/cateid/%d/fromdate/%d/todate/%d", categoryId, startTime.Unix(), endTime.Unix())
}

func (s *Scraper) GetPage(startTime time.Time, endTime time.Time, categoryId int64) (string, error) {
	// Random delay between 0.2 and 2 seconds
	delay := time.Duration(200+rand.Intn(1800)) * time.Millisecond
	time.Sleep(delay)
	
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
		title := e.ChildText("h1.title-detail")
		description := e.ChildText("p.description")
		
		// Parse the published date - VNExpress uses meta tags for accurate dates
		var publishedDate time.Time
		
		// Try to get from meta tag first (most reliable)
		metaDate := e.ChildAttr("meta[name='pubdate']", "content")
		if metaDate == "" {
			metaDate = e.ChildAttr("meta[property='article:published_time']", "content")
		}
		
		if metaDate != "" {
			// Meta tags usually use ISO8601 format
			formats := []string{
				time.RFC3339,
				"2006-01-02T15:04:05-07:00",
				"2006-01-02T15:04:05Z",
			}
			for _, format := range formats {
				if parsed, err := time.Parse(format, metaDate); err == nil {
					publishedDate = parsed
					break
				}
			}
		}
		
	// Fallback to visible date text
	if publishedDate.IsZero() {
		dateStr := e.ChildText("span.date")
		if dateStr != "" {
			// VNExpress format: "Thứ hai, 24/11/2025, 09:35 (GMT+7)"
			// Remove Vietnamese day name and parse the date/time part
			dateStr = strings.TrimSpace(dateStr)
			
			// Remove Vietnamese day name (everything before first comma)
			parts := strings.SplitN(dateStr, ",", 2)
			if len(parts) == 2 {
				dateStr = strings.TrimSpace(parts[1])
			}
			
			// Now dateStr should be like "24/11/2025, 09:35 (GMT+7)"
			// Parse with format: "02/01/2006, 15:04 (GMT+7)"
			parsed, err := time.Parse("2/1/2006, 15:04 (GMT+7)", dateStr)
			if err == nil {
				publishedDate = parsed
			}
		}
	}
	
	log.Printf("Extracted date for '%s': %v (raw meta: %s)\n", title, publishedDate, metaDate)
		
		e.ForEach("article.fck_detail ", func(i int, e *colly.HTMLElement) {
			// Extract all paragraph text from the article
			var contentParts []string
			
			// Get text from all paragraphs in the article
			e.ForEach("p.Normal", func(_ int, p *colly.HTMLElement) {
				text := strings.TrimSpace(p.Text)
				if text != "" {
					contentParts = append(contentParts, text)
				}
			})
			
			// If no paragraphs with class "Normal", try getting all <p> tags
			if len(contentParts) == 0 {
				e.ForEach("p", func(_ int, p *colly.HTMLElement) {
					text := strings.TrimSpace(p.Text)
					// Skip author, photo credit, and other metadata
					if text != "" && !strings.Contains(p.Attr("class"), "author") && 
					   !strings.Contains(p.Attr("class"), "Image") {
						contentParts = append(contentParts, text)
					}
				})
			}
			
			textContent := strings.Join(contentParts, "\n\n")
			
			request := WriteRequest{
				Content:       textContent,
				Title:         title,
				URL:           e.Request.URL.String(),
				Description:   description,
				PublishedDate: publishedDate,
				Label:         s.options.DefaultLabel,
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
	db, err := setupDatabase()
	if err != nil {
		panic(err)
	}
	
	// Configure proxy URL from environment variable `PROXY_URL`.
	// If `PROXY_URL` is empty/unset, proxy will be disabled (no proxy).
	proxyEnv := os.Getenv("PROXY_URL")
	var proxyFunc func(*http.Request) (*url.URL, error)
	if proxyEnv != "" {
		parsedProxyURL, err := url.Parse(proxyEnv)
		if err != nil {
			panic(err)
		}
		proxyFunc = http.ProxyURL(parsedProxyURL)
	} else {
		// No proxy
		proxyFunc = nil
	}
	
	newsCollector := colly.NewCollector(
		colly.CacheDir("./cache"),
		colly.AllowedDomains("vnexpress.net"),
		colly.Async(true),
		colly.Debugger(&debug.LogDebugger{}),
	)
	newsCollector.Limit(&colly.LimitRule{DomainGlob: "*", Parallelism: options.Parallelism})

	// Configure HTTP transport with proxy for colly collector
	newsCollector.WithTransport(&http.Transport{
		Proxy: proxyFunc,
		TLSClientConfig: &tls.Config{
			InsecureSkipVerify: true,
		},
		MaxIdleConns:        100,
		MaxIdleConnsPerHost: 10,
		IdleConnTimeout:     90 * time.Second,
	})

	// Configure retryablehttp client with proxy
	retryClient := retryablehttp.NewClient()
	retryClient.HTTPClient.Transport = &http.Transport{
		Proxy: proxyFunc,
		TLSClientConfig: &tls.Config{
			InsecureSkipVerify: true,
		},
	}

	scraper := &Scraper{
		client:        retryClient,
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
		INSERT INTO articles(url, title, description, content, label, published_date)
		VALUES($1, $2, $3, $4, $5, $6)
		ON CONFLICT (url) DO UPDATE SET
			title = EXCLUDED.title,
			description = EXCLUDED.description,
			content = EXCLUDED.content,
			label = EXCLUDED.label,
			published_date = EXCLUDED.published_date;
	`

	for item := range s.writes {
		// Log start of processing
		log.Printf("Processing Write/Update for: **%s** (label=%s)\n", item.Title, item.Label)

		// Execute the SQL upsert
		_, err := s.db.Exec(upsertSQL, item.URL, item.Title, item.Description, item.Content, item.Label, item.PublishedDate)
		if err != nil {
			log.Printf("Error writing/updating article '%s' to database: %v", item.Title, err)
		} else {
			log.Printf("Successfully wrote/updated article: %s\n", item.Title)
		}
	}
}

// LinkExists checks whether a given URL already exists in the articles table.
func (s *Scraper) LinkExists(url string) (bool, error) {
	var v int
	err := s.db.QueryRow("SELECT 1 FROM articles WHERE url = $1 LIMIT 1", url).Scan(&v)
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
		// Random delay between 0.2 and 2 seconds
		delay := time.Duration(200+rand.Intn(1800)) * time.Millisecond
		time.Sleep(delay)
		
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
