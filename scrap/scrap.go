package scrap

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"html"
	"io"
	"log"
	"math"
	"math/rand/v2"
	"net/url"
	"regexp"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/PuerkitoBio/goquery"
	fhttp "github.com/bogdanfinn/fhttp"
	tls_client "github.com/bogdanfinn/tls-client"
	"github.com/bogdanfinn/tls-client/profiles"
	xhtml "golang.org/x/net/html"
)

// ==========================================
// Precompiled Regex Constants
// ==========================================
var (
	regexUserInfo    = regexp.MustCompile(`(?i)userInfo|user-info`)
	regexHDBadge     = regexp.MustCompile(`(?i)hd-tag|title-hd-badge`)
	regexQualityHD   = regexp.MustCompile(`"quality_(1080|720|2160|4k)`)
	regexIsVertical  = regexp.MustCompile(`(?i)"isVertical"\s*:\s*true|isVerticalVideo\s*=\s*true`)
	regexInfoPiece   = regexp.MustCompile(`(?i)infoPiece|info-piece`)
	regexSubCount    = regexp.MustCompile(`(?i)subscribers-count|subCount|followers`)
	regexSubMatch    = regexp.MustCompile(`([\d\.,]+[KkMm]?)`)
	regexISODuration = regexp.MustCompile(`^PT(?:(\d+)H)?(?:(\d+)M)?(?:(\d+)S)?`)
	regexViewKey     = regexp.MustCompile(`viewkey=([^&]+)`)
	regexAuthorPath  = regexp.MustCompile(`/(videos|photos|playlists|community).*$`)
	regexMultiSpace  = regexp.MustCompile(`\s+`)
)

// ==========================================
// Custom Errors
// ==========================================
var (
	ErrRateLimit       = errors.New("rate limit exceeded")
	ErrCloudflareBlock = errors.New("blocked by Cloudflare Captcha")
	ErrScraping        = errors.New("scraping failed")
)

// ==========================================
// Data Models
// ==========================================

type Author struct {
	Name        string
	URL         string
	AvatarURL   string
	Subscribers string
	Info        map[string]string
	Bio         string
}

type Likes struct {
	Up   int `json:"up"`
	Down int `json:"down"`
}

type VideoMetadata struct {
	URL             string
	VideoID         string
	Title           string
	IsVertical      bool
	IsHD            bool
	DurationSeconds *int
	Views           string
	Likes           Likes
	DateISO         *string
	Tags            []string
	Categories      []string
	ImageURL        *string
	Author          *Author
}

// AuthorOutput matches the serialized author dictionary from VideoMetadata.to_dict().
type AuthorOutput struct {
	Name        *string           `json:"name"`
	AvatarURL   *string           `json:"avatar_url"`
	Bio         *string           `json:"bio"`
	Info        map[string]string `json:"info"`
	Subscribers string            `json:"subscribers"`
}

// MetadataOutput matches the exact JSON schema produced by VideoMetadata.to_dict().
type MetadataOutput struct {
	URL             string       `json:"url"`
	ID              string       `json:"id"`
	Title           string       `json:"title"`
	IsVertical      bool         `json:"is_vertical"`
	IsHD            bool         `json:"is_HD"`
	DurationSeconds *int         `json:"duration_seconds"`
	Views           string       `json:"views"`
	Likes           Likes        `json:"likes"`
	DateISO         *string      `json:"date_iso"`
	Tags            []string     `json:"tags"`
	Categories      []string     `json:"categories"`
	ImageURL        *string      `json:"image_url"`
	Author          AuthorOutput `json:"author"`
}

// ToOutput converts VideoMetadata to the standardized structure expected by main.go.
func (m *VideoMetadata) ToOutput() *MetadataOutput {
	tags := m.Tags
	if tags == nil {
		tags = []string{}
	}
	categories := m.Categories
	if categories == nil {
		categories = []string{}
	}

	out := &MetadataOutput{
		URL:             m.URL,
		ID:              m.VideoID,
		Title:           m.Title,
		IsVertical:      m.IsVertical,
		IsHD:            m.IsHD,
		DurationSeconds: m.DurationSeconds,
		Views:           m.Views,
		Likes:           m.Likes,
		DateISO:         m.DateISO,
		Tags:            tags,
		Categories:      categories,
		ImageURL:        m.ImageURL,
		Author: AuthorOutput{
			Info:        map[string]string{},
			Subscribers: "Unknown",
		},
	}

	if m.Author != nil {
		name := m.Author.Name
		bio := m.Author.Bio
		out.Author.Name = &name
		out.Author.Bio = &bio
		if m.Author.AvatarURL != "" {
			av := m.Author.AvatarURL
			out.Author.AvatarURL = &av
		}
		if m.Author.Info != nil {
			infoCopy := make(map[string]string, len(m.Author.Info))
			for k, v := range m.Author.Info {
				infoCopy[k] = v
			}
			out.Author.Info = infoCopy
		}
		if m.Author.Subscribers != "" {
			out.Author.Subscribers = m.Author.Subscribers
		}
	}

	return out
}

// Clone deep-copies MetadataOutput so downstream mutations do not alias.
func (o *MetadataOutput) Clone() *MetadataOutput {
	cp := *o
	if o.DurationSeconds != nil {
		v := *o.DurationSeconds
		cp.DurationSeconds = &v
	}
	if o.DateISO != nil {
		v := *o.DateISO
		cp.DateISO = &v
	}
	if o.ImageURL != nil {
		v := *o.ImageURL
		cp.ImageURL = &v
	}
	cp.Tags = append([]string{}, o.Tags...)
	cp.Categories = append([]string{}, o.Categories...)
	if o.Author.Name != nil {
		v := *o.Author.Name
		cp.Author.Name = &v
	}
	if o.Author.AvatarURL != nil {
		v := *o.Author.AvatarURL
		cp.Author.AvatarURL = &v
	}
	if o.Author.Bio != nil {
		v := *o.Author.Bio
		cp.Author.Bio = &v
	}
	infoCopy := make(map[string]string, len(o.Author.Info))
	for k, v := range o.Author.Info {
		infoCopy[k] = v
	}
	cp.Author.Info = infoCopy
	return &cp
}

// ==========================================
// Network Components
// ==========================================

// RateLimiter is a thread-safe sliding window rate limiter.
type RateLimiter struct {
	maxRequests int
	period      time.Duration
	timestamps  []time.Time
	mu          sync.Mutex
}

func NewRateLimiter(maxRequests int, periodSeconds int) *RateLimiter {
	return &RateLimiter{
		maxRequests: maxRequests,
		period:      time.Duration(periodSeconds) * time.Second,
		timestamps:  make([]time.Time, 0, maxRequests),
	}
}

func (r *RateLimiter) WaitIfNeeded(ctx context.Context) error {
	for {
		r.mu.Lock()
		now := time.Now()
		cutoff := 0
		for cutoff < len(r.timestamps) && now.Sub(r.timestamps[cutoff]) >= r.period {
			cutoff++
		}
		if cutoff > 0 {
			copy(r.timestamps, r.timestamps[cutoff:])
			r.timestamps = r.timestamps[:len(r.timestamps)-cutoff]
		}

		if len(r.timestamps) < r.maxRequests {
			r.timestamps = append(r.timestamps, time.Now())
			r.mu.Unlock()
			return nil
		}

		sleepDuration := r.period - now.Sub(r.timestamps[0])
		r.mu.Unlock()

		if sleepDuration > 0 {
			log.Printf("DEBUG - Rate limit reached. Sleeping for %.2fs.", sleepDuration.Seconds())
			timer := time.NewTimer(sleepDuration)
			select {
			case <-ctx.Done():
				timer.Stop()
				return ctx.Err()
			case <-timer.C:
			}
		}
	}
}

type HTTPClientConfig struct {
	RateLimiter   *RateLimiter
	MaxRetries    int
	BackoffFactor float64
	Proxy         string
}

// HTTPClient wraps tls-client with Chrome 120 impersonation, retries, and session resets.
type HTTPClient struct {
	rateLimiter   *RateLimiter
	maxRetries    int
	backoffFactor float64
	proxyURL      string

	mu      sync.Mutex
	session tls_client.HttpClient
}

func NewHTTPClient(cfg HTTPClientConfig) (*HTTPClient, error) {
	if cfg.BackoffFactor <= 0 {
		cfg.BackoffFactor = 2.0
	}

	proxy := strings.TrimSpace(cfg.Proxy)
	var normalizedProxy string
	if proxy != "" {
		if strings.HasPrefix(proxy, "socks5://") ||
			strings.HasPrefix(proxy, "http://") ||
			strings.HasPrefix(proxy, "https://") {
			normalizedProxy = proxy
		} else if !strings.Contains(proxy, "://") {
			normalizedProxy = "http://" + proxy
		}
	}

	c := &HTTPClient{
		rateLimiter:   cfg.RateLimiter,
		maxRetries:    cfg.MaxRetries,
		backoffFactor: cfg.BackoffFactor,
		proxyURL:      normalizedProxy,
	}
	if err := c.initSession(); err != nil {
		return nil, err
	}
	return c, nil
}

func (c *HTTPClient) initSession() error {
	c.mu.Lock()
	defer c.mu.Unlock()

	if c.session != nil {
		c.session.CloseIdleConnections()
	}

	jar := tls_client.NewCookieJar()
	options := []tls_client.HttpClientOption{
		tls_client.WithTimeoutSeconds(30),
		tls_client.WithClientProfile(profiles.Chrome_120),
		tls_client.WithCookieJar(jar),
	}
	if c.proxyURL != "" {
		options = append(options, tls_client.WithProxyUrl(c.proxyURL))
	}

	client, err := tls_client.NewHttpClient(tls_client.NewNoopLogger(), options...)
	if err != nil {
		return fmt.Errorf("failed to initialize tls client: %w", err)
	}
	c.session = client
	return nil
}

// ResetSession destroys and rebuilds the HTTP session to clear flagged/broken states.
func (c *HTTPClient) ResetSession() {
	if err := c.initSession(); err != nil {
		log.Printf("WARNING - Failed to reset HTTP session: %v", err)
	}
}

func (c *HTTPClient) Close() {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.session != nil {
		c.session.CloseIdleConnections()
		c.session = nil
	}
}

func (c *HTTPClient) Get(ctx context.Context, targetURL string) (string, error) {
	for attempt := 0; attempt <= c.maxRetries; attempt++ {
		if c.rateLimiter != nil {
			if err := c.rateLimiter.WaitIfNeeded(ctx); err != nil {
				return "", err
			}
		}

		body, err := c.doGet(ctx, targetURL)
		if err == nil {
			return body, nil
		}

		if errors.Is(err, ErrCloudflareBlock) {
			log.Printf("ERROR - %v", err)
			c.ResetSession()
			return "", err
		}

		if attempt == c.maxRetries {
			return "", fmt.Errorf("%w: failed to fetch %s after %d retries: %v", ErrScraping, targetURL, c.maxRetries, err)
		}

		sleepSeconds := math.Pow(c.backoffFactor, float64(attempt))
		sleepDur := time.Duration(sleepSeconds * float64(time.Second))
		log.Printf("WARNING - Request failed (%v). Retrying in %.2fs...", err, sleepSeconds)

		// Destroy bad session/socket state before next retry
		c.ResetSession()

		timer := time.NewTimer(sleepDur)
		select {
		case <-ctx.Done():
			timer.Stop()
			return "", ctx.Err()
		case <-timer.C:
		}
	}
	return "", fmt.Errorf("%w: exhausted retries for %s", ErrScraping, targetURL)
}

func (c *HTTPClient) doGet(ctx context.Context, targetURL string) (string, error) {
	c.mu.Lock()
	session := c.session
	c.mu.Unlock()
	if session == nil {
		return "", fmt.Errorf("%w: HTTP session is closed", ErrScraping)
	}

	reqCtx, cancel := context.WithTimeout(ctx, 35*time.Second)
	defer cancel()

	req, err := fhttp.NewRequestWithContext(reqCtx, fhttp.MethodGet, targetURL, nil)
	if err != nil {
		return "", err
	}

	req.Header = fhttp.Header{
		"accept":                    {"text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.7"},
		"accept-language":           {"en-US,en;q=0.9"},
		"sec-ch-ua":                 {`"Not_A Brand";v="8", "Chromium";v="120", "Google Chrome";v="120"`},
		"sec-ch-ua-mobile":          {"?0"},
		"sec-ch-ua-platform":        {`"Windows"`},
		"sec-fetch-dest":            {"document"},
		"sec-fetch-mode":            {"navigate"},
		"sec-fetch-site":            {"none"},
		"sec-fetch-user":            {"?1"},
		"upgrade-insecure-requests": {"1"},
		"user-agent":                {"Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"},
		fhttp.HeaderOrderKey: {
			"sec-ch-ua",
			"sec-ch-ua-mobile",
			"sec-ch-ua-platform",
			"upgrade-insecure-requests",
			"user-agent",
			"accept",
			"sec-fetch-site",
			"sec-fetch-mode",
			"sec-fetch-user",
			"sec-fetch-dest",
			"accept-language",
		},
	}

	type fetchOutcome struct {
		body string
		err  error
	}
	resCh := make(chan fetchOutcome, 1)

	go func() {
		resp, doErr := session.Do(req)
		if doErr != nil {
			resCh <- fetchOutcome{err: doErr}
			return
		}
		defer resp.Body.Close()

		if resp.StatusCode == 429 {
			_, _ = io.Copy(io.Discard, resp.Body)
			resCh <- fetchOutcome{err: fmt.Errorf("%w: HTTP 429 Too Many Requests", ErrRateLimit)}
			return
		}
		if resp.StatusCode < 200 || resp.StatusCode >= 300 {
			_, _ = io.Copy(io.Discard, resp.Body)
			resCh <- fetchOutcome{err: fmt.Errorf("HTTP error status %d", resp.StatusCode)}
			return
		}

		rawBytes, readErr := io.ReadAll(resp.Body)
		if readErr != nil {
			resCh <- fetchOutcome{err: readErr}
			return
		}
		body := string(rawBytes)
		if strings.Contains(body, "Just a moment") || strings.Contains(body, "Attention Required") {
			resCh <- fetchOutcome{err: ErrCloudflareBlock}
			return
		}
		resCh <- fetchOutcome{body: body}
	}()

	select {
	case out := <-resCh:
		return out.body, out.err
	case <-reqCtx.Done():
		return "", fmt.Errorf("request timed out or canceled: %w", reqCtx.Err())
	}
}

// ==========================================
// Core Scraper Logic
// ==========================================

type PornhubScraper struct {
	client *HTTPClient
}

func NewPornhubScraper(client *HTTPClient) *PornhubScraper {
	return &PornhubScraper{client: client}
}

// ScrapeVideos processes a list of URLs, resetting the session between iterations.
func (s *PornhubScraper) ScrapeVideos(ctx context.Context, urls []string) []*VideoMetadata {
	results := make([]*VideoMetadata, 0, len(urls))
	for _, u := range urls {
		s.client.ResetSession()
		meta := s.ScrapeVideo(ctx, u)
		results = append(results, meta)
	}
	return results
}

func (s *PornhubScraper) ScrapeVideo(ctx context.Context, targetURL string) *VideoMetadata {
	log.Printf("INFO - Extracting metadata from: %s", targetURL)

	htmlContent, err := s.client.Get(ctx, targetURL)
	if err != nil {
		log.Printf("ERROR - Failed to fetch video page: %v", err)
		return nil
	}

	doc, err := goquery.NewDocumentFromReader(strings.NewReader(htmlContent))
	if err != nil {
		log.Printf("ERROR - Failed to parse HTML for %s: %v", targetURL, err)
		return nil
	}

	pageTitle := doc.Find("title").First().Text()
	if strings.Contains(pageTitle, "Just a moment") || strings.Contains(pageTitle, "Attention Required") {
		log.Printf("ERROR - Blocked by Cloudflare! The IP was challenged.")
		return nil
	}
	if !strings.Contains(htmlContent, "ld+json") {
		log.Printf("ERROR - Page loaded, but no video data found. You might be facing an Age Gate or Captcha.")
		return nil
	}

	videoID := s.extractVideoID(targetURL)
	meta := &VideoMetadata{
		URL:        targetURL,
		VideoID:    videoID,
		Title:      "Unknown",
		Views:      "Unknown",
		Likes:      Likes{Up: 0, Down: 0},
		Tags:       []string{},
		Categories: []string{},
	}

	s.parseLDJSON(doc, meta)
	s.parseDOM(doc, htmlContent, meta)

	if meta.Author != nil && meta.Author.URL != "" {
		s.enrichAuthorDetails(ctx, meta.Author)
	}

	return meta
}

func (s *PornhubScraper) extractVideoID(rawURL string) string {
	parsed, err := url.Parse(rawURL)
	if err == nil && strings.Contains(parsed.RawQuery, "viewkey=") {
		if match := regexViewKey.FindStringSubmatch(parsed.RawQuery); len(match) > 1 {
			return match[1]
		}
	}
	return strconv.Itoa(100000000 + rand.IntN(900000000))
}

func (s *PornhubScraper) parseLDJSON(doc *goquery.Document, meta *VideoMetadata) {
	ldTag := doc.Find(`script[type="application/ld+json"]`).First()
	if ldTag.Length() == 0 {
		return
	}
	rawJSON := strings.TrimSpace(ldTag.Text())
	if rawJSON == "" {
		return
	}

	dec := json.NewDecoder(strings.NewReader(rawJSON))
	dec.UseNumber()

	var data map[string]any
	if err := dec.Decode(&data); err != nil {
		log.Printf("WARNING - Failed to parse ld+json: %v", err)
		return
	}

	if rawTitle, ok := data["name"]; ok && rawTitle != nil {
		if str, isStr := rawTitle.(string); isStr {
			meta.Title = html.UnescapeString(str)
		} else {
			meta.Title = anyToString(rawTitle)
		}
	}

	if thumb, ok := data["thumbnailUrl"].(string); ok && thumb != "" {
		meta.ImageURL = &thumb
	}

	if dur, ok := data["duration"].(string); ok && dur != "" {
		meta.DurationSeconds = ParseISODuration(dur)
	}

	if upDate, ok := data["uploadDate"].(string); ok && upDate != "" {
		meta.DateISO = FormatDateISO(upDate)
	}

	if rawInteractions, ok := data["interactionStatistic"]; ok && rawInteractions != nil {
		var interactions []map[string]any
		switch v := rawInteractions.(type) {
		case []any:
			for _, item := range v {
				if m, ok := item.(map[string]any); ok {
					interactions = append(interactions, m)
				}
			}
		case map[string]any:
			interactions = append(interactions, v)
		}

		for _, stat := range interactions {
			iType, _ := stat["interactionType"].(string)
			countVal, hasCount := stat["userInteractionCount"]
			switch iType {
			case "http://schema.org/WatchAction":
				if hasCount && countVal != nil {
					meta.Views = anyToString(countVal)
				}
			case "http://schema.org/LikeAction":
				if hasCount && countVal != nil {
					if n, ok := anyToInt(countVal); ok {
						meta.Likes.Up = n
					}
				}
			}
		}
	}

	if rawAuthor, ok := data["author"]; ok && rawAuthor != nil {
		switch a := rawAuthor.(type) {
		case string:
			meta.Author = &Author{
				Name:        html.UnescapeString(a),
				Subscribers: "Unknown",
				Info:        map[string]string{},
				Bio:         "Bio not available",
			}
		case map[string]any:
			var rawName any = a["name"]
			if slice, isSlice := rawName.([]any); isSlice && len(slice) > 0 {
				rawName = slice[0]
			}
			cleanName := "Unknown"
			if rawName != nil {
				if s := anyToString(rawName); s != "" {
					cleanName = html.UnescapeString(s)
				}
			}
			authorURL, _ := a["url"].(string)
			meta.Author = &Author{
				Name:        cleanName,
				URL:         authorURL,
				Subscribers: "Unknown",
				Info:        map[string]string{},
				Bio:         "Bio not available",
			}
		}
	}
}

func (s *PornhubScraper) parseDOM(doc *goquery.Document, rawHTML string, meta *VideoMetadata) {
	if meta.Author == nil {
		meta.Author = &Author{
			Name:        "Unknown",
			Subscribers: "Unknown",
			Info:        map[string]string{},
			Bio:         "Bio not available",
		}
	}

	var userInfoDiv *goquery.Selection
	doc.Find("div[class]").EachWithBreak(func(_ int, sel *goquery.Selection) bool {
		if cls, ok := sel.Attr("class"); ok && regexUserInfo.MatchString(cls) {
			userInfoDiv = sel
			return false
		}
		return true
	})

	if userInfoDiv != nil {
		if img := userInfoDiv.Find("img").First(); img.Length() > 0 {
			if src, ok := img.Attr("src"); ok && src != "" {
				meta.Author.AvatarURL = src
			}
		}
		if aTag := userInfoDiv.Find("a[href]").First(); aTag.Length() > 0 && meta.Author.URL == "" {
			href, _ := aTag.Attr("href")
			if strings.Contains(href, "/model/") ||
				strings.Contains(href, "/pornstar/") ||
				strings.Contains(href, "/channels/") ||
				strings.Contains(href, "/users/") {
				meta.Author.URL = href
			}
		}
	}

	if tagsWrapper := doc.Find("div.tagsWrapper").First(); tagsWrapper.Length() > 0 {
		tagsWrapper.Find("a").Each(func(_ int, sel *goquery.Selection) {
			text := strings.TrimSpace(sel.Text())
			if text != "" && !strings.Contains(text, "Suggest") {
				meta.Tags = append(meta.Tags, text)
			}
		})
	}

	if catsWrapper := doc.Find("div.categoriesWrapper").First(); catsWrapper.Length() > 0 {
		catsWrapper.Find("a").Each(func(_ int, sel *goquery.Selection) {
			text := strings.TrimSpace(sel.Text())
			if text != "" && !strings.Contains(text, "Suggest") {
				meta.Categories = append(meta.Categories, text)
			}
		})
	}

	if meta.Likes.Up == 0 {
		if likesTag := doc.Find("span.votesUp").First(); likesTag.Length() > 0 {
			rating, ok := likesTag.Attr("data-rating")
			if !ok || rating == "" {
				rating = strings.TrimSpace(likesTag.Text())
			}
			if val, err := strconv.Atoi(strings.TrimSpace(rating)); err == nil {
				meta.Likes.Up = val
			}
		}
	}

	if meta.Views == "Unknown" {
		if viewsTag := doc.Find("span.count").First(); viewsTag.Length() > 0 {
			if v := strings.TrimSpace(viewsTag.Text()); v != "" {
				meta.Views = v
			}
		}
	}

	hasHDBadge := false
	doc.Find("span[class]").EachWithBreak(func(_ int, sel *goquery.Selection) bool {
		if cls, ok := sel.Attr("class"); ok && regexHDBadge.MatchString(cls) {
			hasHDBadge = true
			return false
		}
		return true
	})
	meta.IsHD = hasHDBadge || regexQualityHD.MatchString(rawHTML)
	meta.IsVertical = regexIsVertical.MatchString(rawHTML)
}

func (s *PornhubScraper) enrichAuthorDetails(ctx context.Context, author *Author) {
	if author.URL == "" {
		return
	}

	// SSRF PREVENTION: Strict Validation of domain before executing external requests
	parsedURL, err := url.Parse(author.URL)
	if err != nil {
		return
	}
	if parsedURL.Scheme != "" && parsedURL.Scheme != "http" && parsedURL.Scheme != "https" {
		return
	}
	if parsedURL.Host != "" && parsedURL.Host != "www.pornhub.com" && parsedURL.Host != "pornhub.com" {
		return
	}

	cleanPath := regexAuthorPath.ReplaceAllString(parsedURL.Path, "")
	cleanPath = strings.TrimRight(cleanPath, "/")
	if cleanPath == "" {
		return
	}

	base, _ := url.Parse("https://www.pornhub.com")
	rel, err := url.Parse(cleanPath + "/about")
	if err != nil {
		return
	}
	aboutURL := base.ResolveReference(rel).String()

	htmlContent, err := s.client.Get(ctx, aboutURL)
	if err != nil {
		log.Printf("WARNING - Failed to extract deep author details: %v", err)
		return
	}

	doc, err := goquery.NewDocumentFromReader(strings.NewReader(htmlContent))
	if err != nil {
		log.Printf("WARNING - Failed to parse author about HTML: %v", err)
		return
	}

	doc.Find("div[class]").Each(func(_ int, piece *goquery.Selection) {
		if cls, ok := piece.Attr("class"); !ok || !regexInfoPiece.MatchString(cls) {
			return
		}
		text := extractTextWithSeparator(piece, " ")
		text = strings.TrimSpace(regexMultiSpace.ReplaceAllString(text, " "))
		if key, val, found := strings.Cut(text, ":"); found {
			author.Info[strings.TrimSpace(key)] = strings.TrimSpace(val)
		} else if text != "" {
			author.Info[text] = ""
		}
	})

	doc.Find("[class]").EachWithBreak(func(_ int, sel *goquery.Selection) bool {
		if cls, ok := sel.Attr("class"); ok && regexSubCount.MatchString(cls) {
			subText := strings.TrimSpace(sel.Text())
			if match := regexSubMatch.FindStringSubmatch(subText); len(match) > 1 {
				author.Subscribers = match[1]
			}
			return false
		}
		return true
	})

	bioTags := doc.Find(".aboutMeText, section.aboutMeSection div.text, .bio, .profile-about-me")
	if bioTags.Length() > 0 {
		bioTag := bioTags.First()
		bioTag.Find("[class]").Each(func(_ int, info *goquery.Selection) {
			if cls, ok := info.Attr("class"); ok && regexInfoPiece.MatchString(cls) {
				info.Remove()
			}
		})

		bioText := extractTextWithSeparator(bioTag, "\n")
		if author.Name != "" && author.Name != "Unknown" {
			prefixRegex := regexp.MustCompile(`(?i)^\s*About\s+` + regexp.QuoteMeta(author.Name) + `[\s:-]*`)
			bioText = strings.TrimSpace(prefixRegex.ReplaceAllString(bioText, ""))
		}
		if bioText != "" {
			author.Bio = bioText
		}
	} else if metaDesc := doc.Find(`meta[name="description"]`).First(); metaDesc.Length() > 0 {
		if content, ok := metaDesc.Attr("content"); ok {
			if trimmed := strings.TrimSpace(content); trimmed != "" {
				author.Bio = trimmed
			}
		}
	}
}

// ==========================================
// Parsing & Utility Helpers
// ==========================================

func ParseISODuration(isoStr string) *int {
	if isoStr == "" {
		return nil
	}
	match := regexISODuration.FindStringSubmatch(strings.ToUpper(isoStr))
	if match == nil {
		return nil
	}
	var h, m, sec int
	if match[1] != "" {
		h, _ = strconv.Atoi(match[1])
	}
	if match[2] != "" {
		m, _ = strconv.Atoi(match[2])
	}
	if match[3] != "" {
		sec, _ = strconv.Atoi(match[3])
	}
	total := h*3600 + m*60 + sec
	return &total
}

var isoLayouts = []string{
	time.RFC3339Nano,
	time.RFC3339,
	"2006-01-02T15:04:05.999999999-07:00",
	"2006-01-02T15:04:05-07:00",
	"2006-01-02T15:04:05.999999999",
	"2006-01-02T15:04:05",
	"2006-01-02 15:04:05-07:00",
	"2006-01-02 15:04:05",
	"2006-01-02",
}

func ParseISODate(raw string) (time.Time, bool) {
	if raw == "" {
		return time.Time{}, false
	}
	normalized := strings.ReplaceAll(strings.TrimSpace(raw), "Z", "+00:00")
	for _, layout := range isoLayouts {
		if t, err := time.Parse(layout, normalized); err == nil {
			// If no zone info was present, time.Parse uses UTC already.
			return t, true
		}
	}
	return time.Time{}, false
}

func FormatDateISO(raw string) *string {
	t, ok := ParseISODate(raw)
	if !ok {
		return nil
	}
	formatted := t.Format("2006-01-02T15:04:05-07:00")
	return &formatted
}

// extractTextWithSeparator replicates BeautifulSoup's get_text(separator=sep, strip=True).
func extractTextWithSeparator(sel *goquery.Selection, sep string) string {
	var parts []string
	var walk func(n *xhtml.Node)
	walk = func(n *xhtml.Node) {
		if n.Type == xhtml.TextNode {
			if t := strings.TrimSpace(n.Data); t != "" {
				parts = append(parts, t)
			}
		}
		for c := n.FirstChild; c != nil; c = c.NextSibling {
			walk(c)
		}
	}
	for _, node := range sel.Nodes {
		walk(node)
	}
	return strings.Join(parts, sep)
}

func anyToString(v any) string {
	switch t := v.(type) {
	case string:
		return t
	case json.Number:
		return t.String()
	case float64:
		return strconv.FormatFloat(t, 'f', -1, 64)
	case int:
		return strconv.Itoa(t)
	case int64:
		return strconv.FormatInt(t, 10)
	default:
		return fmt.Sprint(t)
	}
}

func anyToInt(v any) (int, bool) {
	switch t := v.(type) {
	case json.Number:
		if i, err := t.Int64(); err == nil {
			return int(i), true
		}
		if f, err := t.Float64(); err == nil {
			return int(f), true
		}
	case float64:
		return int(t), true
	case int:
		return t, true
	case int64:
		return int(t), true
	case string:
		if i, err := strconv.Atoi(strings.TrimSpace(t)); err == nil {
			return i, true
		}
	}
	return 0, false
}
