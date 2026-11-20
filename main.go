package main

import (
	"archive/zip"
	"bufio"
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log"
	"net"
	"net/http"
	"net/url"
	"os"
	"os/exec"
	"path/filepath"
	"sort"
	"strings"
	"sync"
	"time"

	"video-downloader/scrap"
	"video-downloader/script"
)

// ------------------------------------------------------------------------------
// Configuration & Models
// ------------------------------------------------------------------------------

type Config struct {
	ProxySources           []string          `json:"proxy_sources"`
	ProxyValidationTimeout int               `json:"proxy_validation_timeout"`
	ProxyMaxLatencySeconds float64           `json:"proxy_max_latency_seconds"`
	ProxyValidationWorkers int               `json:"proxy_validation_workers"`
	ProxyMinSpeedMBps      float64           `json:"proxy_min_speed_mbps"`
	ConcurrentDownloads    int               `json:"concurrent_downloads"`
	MaxRetriesPerVideo     int               `json:"max_retries_per_video"`
	ChunkSizeBytes         int64             `json:"chunk_size_bytes"`
	UserAgent              string            `json:"user_agent"`
	DownloadHeaders        map[string]string `json:"download_headers"`
	TempDir                string            `json:"temp_dir"`
	ValidProxiesFile       string            `json:"valid_proxies_file"`
	URLsFile               string            `json:"urls_file"`
	NoPreviewImageURL      string            `json:"no_preview_image_url"`
	NoAvatarImageURL       string            `json:"no_avatar_image_url"`
}

func loadConfig(path string) (*Config, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return nil, fmt.Errorf("configuration file %s not found or unreadable: %w", path, err)
	}
	var cfg Config
	if err := json.Unmarshal(data, &cfg); err != nil {
		return nil, fmt.Errorf("failed to parse %s: %w", path, err)
	}

	if cfg.ConcurrentDownloads <= 0 {
		cfg.ConcurrentDownloads = 5
	}
	if cfg.MaxRetriesPerVideo <= 0 {
		cfg.MaxRetriesPerVideo = 3
	}
	if cfg.TempDir == "" {
		cfg.TempDir = "temp_downloads"
	}
	if cfg.ValidProxiesFile == "" {
		cfg.ValidProxiesFile = "valid_proxies.txt"
	}
	if cfg.URLsFile == "" {
		cfg.URLsFile = "urls.txt"
	}
	if cfg.NoPreviewImageURL == "" {
		cfg.NoPreviewImageURL = "https://example.com/no-preview.png"
	}
	if cfg.NoAvatarImageURL == "" {
		cfg.NoAvatarImageURL = "https://example.com/no-avatar.png"
	}
	return &cfg, nil
}

type ChunkInfo struct {
	Part      int    `json:"part"`
	Name      string `json:"name"`
	SizeBytes int64  `json:"size_bytes"`
}

type FullMetadata struct {
	*scrap.MetadataOutput
	ThumbnailDownloaded bool        `json:"thumbnail_downloaded"`
	PreviewFilename     *string     `json:"preview_filename"`
	AvatarDownloaded    bool        `json:"avatar_downloaded"`
	AvatarFilename      *string     `json:"avatar_filename"`
	Chunks              []ChunkInfo `json:"chunks"`
}

type IndexChunk struct {
	Part      int    `json:"part"`
	URL       string `json:"url"`
	SizeBytes int64  `json:"size_bytes"`
}

type IndexEntry struct {
	VideoID            string        `json:"video_id"`
	Title              string        `json:"title"`
	Author             string        `json:"author"`
	DurationDisplay    string        `json:"duration_display"`
	PublishDateDisplay string        `json:"publish_date_display"`
	PreviewURL         string        `json:"preview_url"`
	DownloadedAlready  bool          `json:"downloaded_already"`
	Deleted            bool          `json:"deleted"`
	Chunks             []IndexChunk  `json:"chunks"`
	Metadata           *FullMetadata `json:"metadata"`
}

type metadataResult struct {
	Meta  *FullMetadata
	Proxy string
}

// ------------------------------------------------------------------------------
// Formatting helpers
// ------------------------------------------------------------------------------

func formatLength(seconds *int) string {
	if seconds == nil {
		return "Unknown length"
	}
	if *seconds < 0 {
		return "Invalid length"
	}
	hours := *seconds / 3600
	minutes := (*seconds % 3600) / 60

	hPlural := "s"
	if hours == 1 {
		hPlural = ""
	}
	mPlural := "s"
	if minutes == 1 {
		mPlural = ""
	}

	hStr := fmt.Sprintf("%d hour%s", hours, hPlural)
	mStr := fmt.Sprintf("%d minute%s", minutes, mPlural)

	if hours == 0 {
		return mStr
	}
	return fmt.Sprintf("%s and %s", hStr, mStr)
}

func formatPublishDate(publishDate *time.Time) string {
	if publishDate == nil {
		return "Unknown date"
	}
	pub := publishDate.UTC()
	now := time.Now().UTC()
	diff := now.Sub(pub)
	if diff < 0 {
		return "just now"
	}

	years := now.Year() - pub.Year()
	if now.Month() < pub.Month() || (now.Month() == pub.Month() && now.Day() < pub.Day()) {
		years--
	}
	months := (now.Year()-pub.Year())*12 + int(now.Month()) - int(pub.Month())
	if now.Day() < pub.Day() {
		months--
	}
	days := int(diff / (24 * time.Hour))

	plural := func(n int) string {
		if n == 1 {
			return ""
		}
		return "s"
	}

	if years >= 1 {
		return fmt.Sprintf("%d year%s ago", years, plural(years))
	}
	if months >= 1 {
		return fmt.Sprintf("%d month%s ago", months, plural(months))
	}
	if days >= 1 {
		return fmt.Sprintf("%d day%s ago", days, plural(days))
	}
	if diff >= time.Hour {
		hours := int(diff / time.Hour)
		return fmt.Sprintf("%d hour%s ago", hours, plural(hours))
	}
	if diff >= time.Minute {
		minutes := int(diff / time.Minute)
		return fmt.Sprintf("%d minute%s ago", minutes, plural(minutes))
	}
	return "just now"
}

// ------------------------------------------------------------------------------
// Image Download
// ------------------------------------------------------------------------------

func downloadImage(ctx context.Context, imgURL string, outputPath string, proxy string) bool {
	if imgURL == "" {
		return false
	}

	transport := &http.Transport{
		DialContext: (&net.Dialer{
			Timeout:   15 * time.Second,
			KeepAlive: 15 * time.Second,
		}).DialContext,
		TLSHandshakeTimeout: 10 * time.Second,
		DisableKeepAlives:   true,
	}
	if proxy != "" {
		if parsedProxy, err := url.Parse(proxy); err == nil {
			transport.Proxy = http.ProxyURL(parsedProxy)
		}
	}
	defer transport.CloseIdleConnections()

	client := &http.Client{
		Transport: transport,
		Timeout:   20 * time.Second,
	}

	const maxRetries = 3
	for attempt := 0; attempt <= maxRetries; attempt++ {
		req, err := http.NewRequestWithContext(ctx, http.MethodGet, imgURL, nil)
		if err != nil {
			log.Printf("WARNING - Failed to create image request for %s: %v", imgURL, err)
			return false
		}

		resp, err := client.Do(req)
		if err == nil {
			if resp.StatusCode >= 200 && resp.StatusCode < 300 {
				f, fileErr := os.Create(outputPath)
				if fileErr != nil {
					resp.Body.Close()
					log.Printf("WARNING - Failed to create image file %s: %v", outputPath, fileErr)
					return false
				}
				_, copyErr := io.Copy(f, resp.Body)
				closeErr := f.Close()
				resp.Body.Close()
				if copyErr == nil && closeErr == nil {
					return true
				}
				_ = os.Remove(outputPath)
				err = errors.Join(copyErr, closeErr)
			} else {
				status := resp.StatusCode
				_, _ = io.Copy(io.Discard, resp.Body)
				resp.Body.Close()
				if status != 500 && status != 502 && status != 503 && status != 504 {
					log.Printf("WARNING - Failed to download image from %s to %s: HTTP %d", imgURL, outputPath, status)
					return false
				}
				err = fmt.Errorf("HTTP %d", status)
			}
		}

		if attempt == maxRetries {
			log.Printf("WARNING - Failed to download image from %s to %s: %v", imgURL, outputPath, err)
			return false
		}

		backoff := time.Duration(500<<attempt) * time.Millisecond
		timer := time.NewTimer(backoff)
		select {
		case <-ctx.Done():
			timer.Stop()
			return false
		case <-timer.C:
		}
	}
	return false
}

// ------------------------------------------------------------------------------
// Dynamic Metadata Fetching Wrapper
// ------------------------------------------------------------------------------

func fetchMetadataWithRotation(ctx context.Context, videoURL string, pm *script.ProxyManager, cfg *Config) (*FullMetadata, string) {
	retries := cfg.MaxRetriesPerVideo

	for attempt := 1; attempt <= retries; attempt++ {
		proxyAddr, ok := pm.GetProxy(ctx, 5*time.Second)
		if !ok {
			pm.Replenish(ctx, 2)
			proxyAddr, ok = pm.GetProxy(ctx, 10*time.Second)
			if !ok {
				log.Printf("ERROR - No proxy available to fetch metadata for %s", videoURL)
				return nil, ""
			}
		}

		log.Printf("INFO - Metadata attempt %d/%d for %s using proxy %s", attempt, retries, videoURL, proxyAddr)

		client, err := scrap.NewHTTPClient(scrap.HTTPClientConfig{
			Proxy:      proxyAddr,
			MaxRetries: 0,
		})
		if err != nil {
			log.Printf("WARNING - Proxy %s failed to initialize HTTP client for %s: %v", proxyAddr, videoURL, err)
			pm.ReportResult(proxyAddr, false)
			continue
		}

		scraper := scrap.NewPornhubScraper(client)
		metaObj := scraper.ScrapeVideo(ctx, videoURL)
		client.Close()

		if metaObj != nil {
			out := metaObj.ToOutput()
			authorName := ""
			if out.Author.Name != nil {
				authorName = *out.Author.Name
			}
			log.Printf("INFO -   '%s' by %s (ID: %s)", out.Title, authorName, out.ID)
			pm.ReportResult(proxyAddr, true)
			return &FullMetadata{MetadataOutput: out}, proxyAddr
		}

		log.Printf("WARNING -   Metadata extraction returned None for %s via proxy %s", videoURL, proxyAddr)
		pm.ReportResult(proxyAddr, false)
	}

	log.Printf("ERROR - Failed to fetch metadata for %s after %d attempts.", videoURL, retries)
	return nil, ""
}

func fetchAllMetadata(ctx context.Context, urls []string, pm *script.ProxyManager, cfg *Config, maxWorkers int) []metadataResult {
	if len(urls) == 0 {
		return nil
	}
	if maxWorkers <= 0 {
		maxWorkers = 5
	}
	if maxWorkers > len(urls) {
		maxWorkers = len(urls)
	}

	jobs := make(chan string)
	resultsCh := make(chan metadataResult, len(urls))

	var wg sync.WaitGroup
	wg.Add(maxWorkers)
	for w := 0; w < maxWorkers; w++ {
		go func() {
			defer wg.Done()
			for u := range jobs {
				meta, workingProxy := fetchMetadataWithRotation(ctx, u, pm, cfg)
				if meta != nil && meta.ID != "" {
					resultsCh <- metadataResult{Meta: meta, Proxy: workingProxy}
				} else {
					log.Printf("WARNING - Skipping %s – No metadata or ID returned.", u)
				}
			}
		}()
	}

	go func() {
		for _, u := range urls {
			jobs <- u
		}
		close(jobs)
		wg.Wait()
		close(resultsCh)
	}()

	results := make([]metadataResult, 0, len(urls))
	for res := range resultsCh {
		results = append(results, res)
	}
	return results
}

// ------------------------------------------------------------------------------
// Video Downloading and Lossless Splitting
// ------------------------------------------------------------------------------

func downloadVideo(ctx context.Context, videoURL string, videoID string, outputPath string, proxy string, cfg *Config) bool {
	args := []string{
		videoURL,
		"--no-playlist",
		"--impersonate", "Chrome-100",
		"--proxy", proxy,
		"--format", "bestvideo+bestaudio/best",
		"--merge-output-format", "mkv",
		"--output", filepath.Join(outputPath, fmt.Sprintf("%s.%%(ext)s", videoID)),
	}

	if len(cfg.DownloadHeaders) > 0 {
		keys := make([]string, 0, len(cfg.DownloadHeaders))
		for k := range cfg.DownloadHeaders {
			keys = append(keys, k)
		}
		sort.Strings(keys)
		for _, k := range keys {
			args = append(args, "--add-header", fmt.Sprintf("%s: %s", k, cfg.DownloadHeaders[k]))
		}
	}

	dlCtx, cancel := context.WithTimeout(ctx, 600*time.Second)
	defer cancel()

	var stderrBuf bytes.Buffer
	cmd := exec.CommandContext(dlCtx, "yt-dlp", args...)
	cmd.Stdout = os.Stdout
	cmd.Stderr = &stderrBuf

	err := cmd.Run()
	if errors.Is(dlCtx.Err(), context.DeadlineExceeded) {
		log.Printf("ERROR - yt-dlp timed out for %s", videoURL)
		return false
	}
	if err != nil {
		log.Printf("ERROR - yt-dlp failed for %s: %s", videoURL, strings.TrimSpace(stderrBuf.String()))
		return false
	}
	return true
}

func findDownloadedFile(downloadDir string, videoID string) (string, bool) {
	pattern := filepath.Join(downloadDir, videoID+".*")
	matches, err := filepath.Glob(pattern)
	if err != nil {
		return "", false
	}
	for _, m := range matches {
		ext := filepath.Ext(m)
		if ext == ".part" || ext == ".ytdl" {
			continue
		}
		if info, statErr := os.Stat(m); statErr == nil && !info.IsDir() {
			return m, true
		}
	}
	return "", false
}

func splitFile(filePath string, videoID string, outputDir string, chunkSize int64) ([]ChunkInfo, error) {
	info, err := os.Stat(filePath)
	if err != nil {
		return nil, err
	}
	fileSize := info.Size()
	if fileSize == 0 {
		return nil, errors.New("empty file, cannot split")
	}
	if chunkSize <= 0 {
		return nil, errors.New("chunk_size_bytes must be positive")
	}

	numParts := int((fileSize + chunkSize - 1) / chunkSize)
	partInfos := make([]ChunkInfo, 0, numParts)

	fin, err := os.Open(filePath)
	if err != nil {
		return nil, err
	}
	defer fin.Close()

	const ioBufSize = 1 << 20 // 1 MiB buffered I/O
	br := bufio.NewReaderSize(fin, ioBufSize)
	copyBuf := make([]byte, ioBufSize)

	for i := 1; i <= numParts; i++ {
		zipFilename := fmt.Sprintf("VID_%s.part%03d.zip", videoID, i)
		zipPath := filepath.Join(outputDir, zipFilename)
		internalChunkName := fmt.Sprintf("VID_%s.part%03d", videoID, i)

		written, writeErr := writeZipChunk(zipPath, internalChunkName, br, chunkSize, copyBuf)
		if writeErr != nil {
			log.Printf("ERROR - Failed to create zip file for %s: %v", internalChunkName, writeErr)
			_ = os.Remove(zipPath)
			return nil, writeErr
		}
		if written == 0 {
			log.Printf("WARNING - No data read for part %d of %s, skipping.", i, videoID)
			_ = os.Remove(zipPath)
			continue
		}

		zipStat, err := os.Stat(zipPath)
		if err != nil {
			return nil, err
		}

		partInfos = append(partInfos, ChunkInfo{
			Part:      i,
			Name:      zipFilename,
			SizeBytes: zipStat.Size(),
		})
	}

	return partInfos, nil
}

func writeZipChunk(zipPath, internalName string, src io.Reader, maxBytes int64, copyBuf []byte) (int64, error) {
	zf, err := os.Create(zipPath)
	if err != nil {
		return 0, err
	}
	defer func() {
		if zf != nil {
			_ = zf.Close()
		}
	}()

	bw := bufio.NewWriterSize(zf, 1<<20)
	zw := zip.NewWriter(bw)

	header := &zip.FileHeader{
		Name:     internalName,
		Method:   zip.Store, // ZIP_STORED: lossless and zero compression CPU overhead
		Modified: time.Now(),
	}
	entryWriter, err := zw.CreateHeader(header)
	if err != nil {
		return 0, err
	}

	written, err := io.CopyBuffer(entryWriter, io.LimitReader(src, maxBytes), copyBuf)
	if err != nil {
		return 0, err
	}
	if err := zw.Close(); err != nil {
		return 0, err
	}
	if err := bw.Flush(); err != nil {
		return 0, err
	}
	if err := zf.Close(); err != nil {
		zf = nil
		return 0, err
	}
	zf = nil
	return written, nil
}

func downloadAndSplit(
	ctx context.Context,
	videoURL string,
	metadata *FullMetadata,
	pm *script.ProxyManager,
	cfg *Config,
	tempDir string,
	preferredProxy string,
) bool {
	videoID := metadata.ID
	const retries = 5
	successDownload := false
	folderName := fmt.Sprintf("VID_%s", videoID)
	usedProxies := make(map[string]struct{})
	var chosenProxy string

	for attempt := 1; attempt <= retries; attempt++ {
		var proxyAddr string
		if attempt == 1 && preferredProxy != "" {
			proxyAddr = preferredProxy
			log.Printf("INFO - First download attempt for %s using proven metadata proxy %s", videoID, proxyAddr)
		} else {
			var pulledProxies []string
			for round := 0; round < 3; round++ {
				for {
					p, ok := pm.GetProxy(ctx, 2*time.Second)
					if !ok {
						break
					}
					if _, used := usedProxies[p]; !used {
						proxyAddr = p
						break
					}
					pulledProxies = append(pulledProxies, p)
				}
				if proxyAddr != "" {
					break
				}
				pm.Replenish(ctx, 10+len(usedProxies))
			}

			for _, pp := range pulledProxies {
				pm.ReportResult(pp, true)
			}

			if proxyAddr == "" {
				log.Printf("ERROR - Unable to obtain a brand new proxy for %s after replenish attempts.", videoID)
				return false
			}
		}

		usedProxies[proxyAddr] = struct{}{}
		chosenProxy = proxyAddr

		log.Printf("INFO - Downloading %s (attempt %d/%d) using proxy %s", videoID, attempt, retries, proxyAddr)
		if downloadVideo(ctx, videoURL, videoID, tempDir, proxyAddr, cfg) {
			successDownload = true
			pm.ReportResult(proxyAddr, true)
			break
		}
		log.Printf("WARNING - Download failed for %s with proxy %s", videoID, proxyAddr)
		pm.ReportResult(proxyAddr, false)
	}

	if !successDownload {
		log.Printf("ERROR - All %d download attempts failed for %s. Skipping.", retries, videoID)
		return false
	}

	downloadedFile, found := findDownloadedFile(tempDir, videoID)
	if !found {
		log.Printf("ERROR - Downloaded file not found for %s.", videoID)
		return false
	}
	defer func() { _ = os.Remove(downloadedFile) }()

	if err := os.MkdirAll(folderName, 0755); err != nil {
		log.Printf("ERROR - Failed to create folder %s: %v", folderName, err)
		return false
	}

	var assetWG sync.WaitGroup
	thumbnailFilename := "preview.png"
	previewPath := filepath.Join(folderName, thumbnailFilename)
	if metadata.ImageURL != nil && *metadata.ImageURL != "" {
		assetWG.Add(1)
		go func(imgURL string) {
			defer assetWG.Done()
			ok := downloadImage(ctx, imgURL, previewPath, chosenProxy)
			metadata.ThumbnailDownloaded = ok
			if ok {
				name := thumbnailFilename
				metadata.PreviewFilename = &name
			}
		}(*metadata.ImageURL)
	}

	avatarFilename := "avatar.png"
	avatarPath := filepath.Join(folderName, avatarFilename)
	if metadata.Author.AvatarURL != nil && *metadata.Author.AvatarURL != "" {
		assetWG.Add(1)
		go func(avURL string) {
			defer assetWG.Done()
			ok := downloadImage(ctx, avURL, avatarPath, chosenProxy)
			metadata.AvatarDownloaded = ok
			if ok {
				name := avatarFilename
				metadata.AvatarFilename = &name
			}
		}(*metadata.Author.AvatarURL)
	}
	assetWG.Wait()

	chunkInfo, err := splitFile(downloadedFile, videoID, folderName, cfg.ChunkSizeBytes)
	if err != nil {
		log.Printf("ERROR - Splitting failed for %s: %v", videoID, err)
		_ = os.RemoveAll(folderName)
		return false
	}

	metadata.Chunks = chunkInfo
	metadataPath := filepath.Join(folderName, "metadata.json")
	if err := writeJSONFile(metadataPath, metadata); err != nil {
		log.Printf("ERROR - Failed writing %s: %v", metadataPath, err)
		return false
	}
	return true
}

// ------------------------------------------------------------------------------
// Git operations
// ------------------------------------------------------------------------------

func runGitCommand(args ...string) bool {
	var stderrBuf bytes.Buffer
	cmd := exec.Command("git", args...)
	cmd.Stderr = &stderrBuf
	if err := cmd.Run(); err != nil {
		log.Printf("ERROR - Git command 'git %s' failed: %s", strings.Join(args, " "), strings.TrimSpace(stderrBuf.String()))
		return false
	}
	return true
}

func commitAndPush() bool {
	branch := os.Getenv("GITHUB_REF_NAME")
	if branch == "" {
		branch = "main"
	}
	repo := os.Getenv("GITHUB_REPOSITORY")
	if repo == "" {
		repo = "owner/repo"
	}
	log.Printf("INFO - Committing and pushing to %s on branch %s", repo, branch)

	runGitCommand("config", "user.name", "github-actions[bot]")
	runGitCommand("config", "user.email", "github-actions[bot]@users.noreply.github.com")

	if !runGitCommand("add", "-A") {
		return false
	}

	var outBuf bytes.Buffer
	commitCmd := exec.Command("git", "commit", "-m", "Add video chunks [skip ci]")
	commitCmd.Stdout = &outBuf
	commitCmd.Stderr = &outBuf
	if err := commitCmd.Run(); err != nil {
		outputStr := strings.ToLower(outBuf.String())
		if strings.Contains(outputStr, "nothing to commit") {
			log.Printf("INFO - Nothing to commit. Repository is up-to-date.")
			return true
		}
		log.Printf("ERROR - Commit failed: %s", strings.TrimSpace(outBuf.String()))
		return false
	}

	return runGitCommand("push", "origin", "HEAD:"+branch)
}

// ------------------------------------------------------------------------------
// JSON & File Helpers
// ------------------------------------------------------------------------------

func writeJSONFile(path string, v any) error {
	f, err := os.Create(path)
	if err != nil {
		return err
	}
	defer f.Close()

	bw := bufio.NewWriterSize(f, 64*1024)
	enc := json.NewEncoder(bw)
	enc.SetEscapeHTML(false)
	enc.SetIndent("", "  ")
	if err := enc.Encode(v); err != nil {
		return err
	}
	return bw.Flush()
}

type existingIndexProbe struct {
	VideoID  string `json:"video_id"`
	Metadata struct {
		URL string `json:"url"`
	} `json:"metadata"`
}

func loadExistingIndex(indexPath string) ([]json.RawMessage, map[string]struct{}, map[string]struct{}) {
	existingURLs := make(map[string]struct{})
	existingIDs := make(map[string]struct{})

	data, err := os.ReadFile(indexPath)
	if err != nil {
		return nil, existingURLs, existingIDs
	}

	var entries []json.RawMessage
	if err := json.Unmarshal(data, &entries); err != nil {
		log.Printf("WARNING - Existing index.json is corrupted or empty. Starting with an empty index.")
		return nil, existingURLs, existingIDs
	}

	for _, raw := range entries {
		var probe existingIndexProbe
		if err := json.Unmarshal(raw, &probe); err == nil {
			if probe.Metadata.URL != "" {
				existingURLs[probe.Metadata.URL] = struct{}{}
			}
			if probe.VideoID != "" {
				existingIDs[probe.VideoID] = struct{}{}
			}
		}
	}
	return entries, existingURLs, existingIDs
}

// ------------------------------------------------------------------------------
// Main execution
// ------------------------------------------------------------------------------

func main() {
	log.SetFlags(log.LstdFlags)
	ctx := context.Background()

	cfg, err := loadConfig("config.json")
	if err != nil {
		log.Fatalf("FATAL - %v", err)
	}
	if err := os.MkdirAll(cfg.TempDir, 0755); err != nil {
		log.Fatalf("FATAL - Failed to create temp dir %s: %v", cfg.TempDir, err)
	}
	const indexPath = "index.json"

	// ---------------------------------------------------
	// 1. Load index & Read URLs
	// ---------------------------------------------------
	existingIndex, existingURLs, existingIDs := loadExistingIndex(indexPath)

	urlsBytes, err := os.ReadFile(cfg.URLsFile)
	if err != nil {
		log.Fatalf("FATAL - URLs file '%s' not found.", cfg.URLsFile)
	}

	seenURLs := make(map[string]struct{})
	var videoURLs []string
	scanner := bufio.NewScanner(bytes.NewReader(urlsBytes))
	for scanner.Scan() {
		u := strings.TrimSpace(scanner.Text())
		if u == "" {
			continue
		}
		if _, seen := seenURLs[u]; seen {
			continue
		}
		seenURLs[u] = struct{}{}
		if _, exists := existingURLs[u]; !exists {
			videoURLs = append(videoURLs, u)
		}
	}

	if len(videoURLs) == 0 {
		log.Printf("INFO - No new URLs to process. Exiting.")
		return
	}

	// ---------------------------------------------------
	// 2. Acquire & Validate Proxies via script/proxy.go
	// ---------------------------------------------------
	pm := script.NewProxyManager(script.ProxyConfig{
		ExtraSources:      cfg.ProxySources,
		ValidProxiesFile:  cfg.ValidProxiesFile,
		ValidationTimeout: time.Duration(cfg.ProxyValidationTimeout) * time.Second,
		MaxLatency:        time.Duration(cfg.ProxyMaxLatencySeconds * float64(time.Second)),
		Workers:           cfg.ProxyValidationWorkers,
		MinSpeedMBps:      cfg.ProxyMinSpeedMBps,
	})

	requiredProxies := len(videoURLs) * 2
	log.Printf("INFO - Acquiring working proxies for %d new videos upfront...", len(videoURLs))
	pm.Replenish(ctx, requiredProxies)

	if pm.IsEmpty() {
		log.Fatalf("FATAL - No proxies available. Aborting.")
	}

	// ---------------------------------------------------
	// 3. Fetch Metadata Using Dynamic Proxy Rotation
	// ---------------------------------------------------
	log.Printf("INFO - Fetching metadata for %d videos dynamically rotating through proxies...", len(videoURLs))
	videosMetadataTuples := fetchAllMetadata(ctx, videoURLs, pm, cfg, cfg.ConcurrentDownloads)
	if len(videosMetadataTuples) == 0 {
		_ = pm.SaveProxies()
		log.Fatalf("FATAL - Failed to fetch metadata for any video. Exiting.")
	}

	// ---------------------------------------------------
	// 4. Concurrent Download & Split
	// ---------------------------------------------------
	concurrentWorkers := cfg.ConcurrentDownloads
	log.Printf("INFO - Starting concurrent downloads processing with %d workers.", concurrentWorkers)

	var pending []metadataResult
	for _, item := range videosMetadataTuples {
		if _, exists := existingIDs[item.Meta.ID]; exists {
			log.Printf("INFO - Video %s already exists in index.json, skipping download.", item.Meta.ID)
			continue
		}
		pending = append(pending, item)
	}

	successIDs := make(map[string]struct{})
	if len(pending) > 0 {
		workerCount := min(concurrentWorkers, len(pending))
		jobs := make(chan metadataResult)
		var (
			wg sync.WaitGroup
			mu sync.Mutex
		)

		wg.Add(workerCount)
		for w := 0; w < workerCount; w++ {
			go func() {
				defer wg.Done()
				for item := range jobs {
					ok := downloadAndSplit(ctx, item.Meta.URL, item.Meta, pm, cfg, cfg.TempDir, item.Proxy)
					if ok {
						mu.Lock()
						successIDs[item.Meta.ID] = struct{}{}
						mu.Unlock()
					} else {
						log.Printf("ERROR - Processing failed for video %s.", item.Meta.ID)
					}
				}
			}()
		}

		for _, item := range pending {
			jobs <- item
		}
		close(jobs)
		wg.Wait()
	}

	// Persist verified working proxies after video processing (prior to git commit/push).
	if err := pm.SaveProxies(); err != nil {
		log.Printf("WARNING - Failed to save validated proxies: %v", err)
	}

	if len(successIDs) == 0 {
		log.Printf("INFO - No new videos processed successfully. Cleaning up.")
		_ = os.RemoveAll(cfg.TempDir)
		return
	}

	// ---------------------------------------------------
	// 5. Generate index.json
	// ---------------------------------------------------
	repo := os.Getenv("GITHUB_REPOSITORY")
	if repo == "" {
		repo = "owner/repo"
	}
	branch := os.Getenv("GITHUB_REF_NAME")
	if branch == "" {
		branch = "main"
	}

	finalIndex := make([]any, 0, len(existingIndex)+len(successIDs))
	for _, raw := range existingIndex {
		finalIndex = append(finalIndex, raw)
	}

	newEntriesCount := 0
	for _, item := range videosMetadataTuples {
		meta := item.Meta
		if _, ok := successIDs[meta.ID]; !ok {
			continue
		}

		folderName := fmt.Sprintf("VID_%s", meta.ID)
		chunksWithURLs := make([]IndexChunk, 0, len(meta.Chunks))
		for _, c := range meta.Chunks {
			chunksWithURLs = append(chunksWithURLs, IndexChunk{
				Part:      c.Part,
				URL:       fmt.Sprintf("https://raw.githubusercontent.com/%s/%s/%s/%s", repo, branch, folderName, c.Name),
				SizeBytes: c.SizeBytes,
			})
		}

		finalPreviewURL := cfg.NoPreviewImageURL
		if meta.ThumbnailDownloaded && meta.PreviewFilename != nil {
			finalPreviewURL = fmt.Sprintf("https://raw.githubusercontent.com/%s/%s/%s/%s", repo, branch, folderName, *meta.PreviewFilename)
		}

		finalAvatarURL := cfg.NoAvatarImageURL
		if meta.AvatarDownloaded && meta.AvatarFilename != nil {
			finalAvatarURL = fmt.Sprintf("https://raw.githubusercontent.com/%s/%s/%s/%s", repo, branch, folderName, *meta.AvatarFilename)
		}

		indexMetaOut := meta.MetadataOutput.Clone()
		indexMetaOut.ImageURL = &finalPreviewURL
		indexMetaOut.Author.AvatarURL = &finalAvatarURL

		indexMeta := &FullMetadata{
			MetadataOutput:      indexMetaOut,
			ThumbnailDownloaded: meta.ThumbnailDownloaded,
			PreviewFilename:     meta.PreviewFilename,
			AvatarDownloaded:    meta.AvatarDownloaded,
			AvatarFilename:      meta.AvatarFilename,
			Chunks:              meta.Chunks,
		}

		var pubDatePtr *time.Time
		if indexMeta.DateISO != nil && *indexMeta.DateISO != "" {
			if parsed, ok := scrap.ParseISODate(*indexMeta.DateISO); ok {
				pubDatePtr = &parsed
			} else {
				log.Printf("WARNING - Could not parse publish date '%s' for video %s", *indexMeta.DateISO, indexMeta.ID)
			}
		}

		authorName := ""
		if indexMeta.Author.Name != nil {
			authorName = *indexMeta.Author.Name
		}

		finalIndex = append(finalIndex, IndexEntry{
			VideoID:            indexMeta.ID,
			Title:              indexMeta.Title,
			Author:             authorName,
			DurationDisplay:    formatLength(indexMeta.DurationSeconds),
			PublishDateDisplay: formatPublishDate(pubDatePtr),
			PreviewURL:         finalPreviewURL,
			DownloadedAlready:  false,
			Deleted:            false,
			Chunks:             chunksWithURLs,
			Metadata:           indexMeta,
		})
		newEntriesCount++
	}

	if err := writeJSONFile(indexPath, finalIndex); err != nil {
		log.Fatalf("FATAL - Failed to write index.json: %v", err)
	}
	log.Printf("INFO - Generated index.json with %d entries (newly added: %d).", len(finalIndex), newEntriesCount)

	// ---------------------------------------------------
	// 6. Commit and push
	// ---------------------------------------------------
	if !commitAndPush() {
		log.Fatalf("FATAL - Git push failed.")
	}

	_ = os.RemoveAll(cfg.TempDir)
	log.Printf("INFO - All done.")
}
