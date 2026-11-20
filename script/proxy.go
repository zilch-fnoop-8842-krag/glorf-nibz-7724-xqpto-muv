package script

import (
	"bufio"
	"bytes"
	"compress/gzip"
	"context"
	"fmt"
	"io"
	"log"
	"math/rand/v2"
	"net"
	"net/http"
	"net/url"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"sync"
	"time"

	fhttp "github.com/bogdanfinn/fhttp"
	tls_client "github.com/bogdanfinn/tls-client"
	"github.com/bogdanfinn/tls-client/profiles"
)

const (
	// DefaultMinSpeedBytesPerSec enforces >= 1.5 MB/s (1.5 MiB/s) download throughput.
	DefaultMinSpeedBytesPerSec = 1.5 * 1024 * 1024

	defaultValidationWorkers = 200
	defaultValidationTimeout = 8 * time.Second
	defaultMaxLatency        = 8 * time.Second
	maxProbeReadBytes        = 4 * 1024 * 1024
	targetSpeedSampleBytes   = int64(1.5 * 1024 * 1024)
	minValidSampleBytes      = 256 * 1024
)

// ProxySource defines a public proxy feed and the default protocol for bare IP:PORT entries.
type ProxySource struct {
	URL           string
	DefaultScheme string
}

// DefaultProxySources lists 10 distinct proxy sources aggregated into the pool.
var DefaultProxySources = []ProxySource{
	{URL: "https://raw.githubusercontent.com/TheSpeedX/SOCKS-List/master/http.txt", DefaultScheme: "http"},
	{URL: "https://raw.githubusercontent.com/Skillter/ProxyGather/refs/heads/master/proxies/working-proxies-http.txt", DefaultScheme: "http"},
	{URL: "https://raw.githubusercontent.com/proxygenerator1/ProxyGenerator/main/MostStable/http.txt", DefaultScheme: "http"},
	{URL: "https://raw.githubusercontent.com/wiki/gfpcom/free-proxy-list/lists/http.txt", DefaultScheme: "http"},
	{URL: "https://raw.githubusercontent.com/Thordata/awesome-free-proxy-list/refs/heads/main/proxies/top-http.txt", DefaultScheme: "http"},
	{URL: "https://cdn.jsdelivr.net/gh/proxifly/free-proxy-list@main/proxies/protocols/https/data.txt", DefaultScheme: "http"},
	{URL: "https://cdn.jsdelivr.net/gh/proxifly/free-proxy-list@main/proxies/protocols/http/data.txt", DefaultScheme: "http"},
	{URL: "https://cdn.jsdelivr.net/gh/proxifly/free-proxy-list@main/proxies/protocols/socks5/data.txt", DefaultScheme: "socks5"},
	{URL: "https://raw.githubusercontent.com/TheSpeedX/SOCKS-List/master/socks5.txt", DefaultScheme: "socks5"},
	{URL: "https://raw.githubusercontent.com/monosans/proxy-list/main/proxies/http.txt", DefaultScheme: "http"},
}

var pornhubProbeTargets = []string{
	"https://www.pornhub.com/",
	"https://www.pornhub.com/video",
	"https://www.pornhub.com/categories",
}

// ProxyConfig configures the proxy pool and validation thresholds.
type ProxyConfig struct {
	ExtraSources      []string
	ValidProxiesFile  string
	ValidationTimeout time.Duration
	MaxLatency        time.Duration
	Workers           int
	MinSpeedMBps      float64
}

type validatedProxy struct {
	URL        string
	SpeedBps   float64
	LastTested time.Time
}

// ProxyManager is a thread-safe proxy pool that collects, validates against pornhub.com,
// rotates, and persists high-speed proxies.
type ProxyManager struct {
	sources             []ProxySource
	validProxiesFile    string
	timeout             time.Duration
	maxLatency          time.Duration
	workers             int
	minSpeedBytesPerSec float64
	sourceClient        *http.Client

	mu              sync.Mutex
	queue           []string
	notEmpty        chan struct{}
	verified        map[string]validatedProxy
	testedSet       map[string]struct{}
	cachedPool      []string
	persistedLoaded bool
	replenishing    bool
	replenishCh     chan struct{}
}

// NewProxyManager constructs a ProxyManager configured with 10+ deduplicated sources.
func NewProxyManager(cfg ProxyConfig) *ProxyManager {
	timeout := cfg.ValidationTimeout
	if timeout <= 0 {
		timeout = defaultValidationTimeout
	}
	maxLatency := cfg.MaxLatency
	if maxLatency <= 0 {
		maxLatency = defaultMaxLatency
	}
	workers := cfg.Workers
	if workers <= 0 {
		workers = defaultValidationWorkers
	}
	minSpeed := DefaultMinSpeedBytesPerSec
	if cfg.MinSpeedMBps > 0 {
		minSpeed = cfg.MinSpeedMBps * 1024 * 1024
	}
	validFile := cfg.ValidProxiesFile
	if validFile == "" {
		validFile = "valid_proxies.txt"
	}

	// Aggregate DefaultProxySources (10 sources) + any extra configured sources without duplicates.
	seenSources := make(map[string]struct{}, len(DefaultProxySources)+len(cfg.ExtraSources))
	sources := make([]ProxySource, 0, len(DefaultProxySources)+len(cfg.ExtraSources))
	for _, s := range DefaultProxySources {
		if _, ok := seenSources[s.URL]; !ok {
			seenSources[s.URL] = struct{}{}
			sources = append(sources, s)
		}
	}
	for _, extra := range cfg.ExtraSources {
		u := strings.TrimSpace(extra)
		if u == "" {
			continue
		}
		if _, ok := seenSources[u]; !ok {
			seenSources[u] = struct{}{}
			sources = append(sources, ProxySource{URL: u, DefaultScheme: "http"})
		}
	}

	return &ProxyManager{
		sources:             sources,
		validProxiesFile:    validFile,
		timeout:             timeout,
		maxLatency:          maxLatency,
		workers:             workers,
		minSpeedBytesPerSec: minSpeed,
		sourceClient: &http.Client{
			Timeout: 15 * time.Second,
			Transport: &http.Transport{
				MaxIdleConns:        32,
				MaxIdleConnsPerHost: 8,
				IdleConnTimeout:     30 * time.Second,
				TLSHandshakeTimeout: 10 * time.Second,
			},
		},
		notEmpty:  make(chan struct{}),
		verified:  make(map[string]validatedProxy),
		testedSet: make(map[string]struct{}),
	}
}

// Len returns the number of proxies currently queued for checkout.
func (pm *ProxyManager) Len() int {
	pm.mu.Lock()
	defer pm.mu.Unlock()
	return len(pm.queue)
}

// IsEmpty reports whether the active proxy queue has no available proxies.
func (pm *ProxyManager) IsEmpty() bool {
	return pm.Len() == 0
}

// VerifiedCount returns the number of currently verified working proxies.
func (pm *ProxyManager) VerifiedCount() int {
	pm.mu.Lock()
	defer pm.mu.Unlock()
	return len(pm.verified)
}

// GetProxy pops a verified proxy from the queue, blocking up to timeout if empty.
func (pm *ProxyManager) GetProxy(ctx context.Context, timeout time.Duration) (string, bool) {
	timer := time.NewTimer(timeout)
	defer timer.Stop()

	for {
		pm.mu.Lock()
		if len(pm.queue) > 0 {
			p := pm.queue[0]
			pm.queue[0] = ""
			pm.queue = pm.queue[1:]
			pm.mu.Unlock()
			return p, true
		}
		waitCh := pm.notEmpty
		pm.mu.Unlock()

		select {
		case <-waitCh:
			continue
		case <-timer.C:
			return "", false
		case <-ctx.Done():
			return "", false
		}
	}
}

// ReportResult returns working proxies to the pool or evicts failed proxies from persistence.
func (pm *ProxyManager) ReportResult(proxy string, success bool) {
	if proxy == "" {
		return
	}
	pm.mu.Lock()
	if success {
		if _, exists := pm.verified[proxy]; !exists {
			pm.verified[proxy] = validatedProxy{URL: proxy, LastTested: time.Now()}
		}
		pm.pushLocked(proxy)
	} else {
		delete(pm.verified, proxy)
	}
	pm.mu.Unlock()
}

func (pm *ProxyManager) pushLocked(proxy string) {
	wasEmpty := len(pm.queue) == 0
	pm.queue = append(pm.queue, proxy)
	if wasEmpty {
		close(pm.notEmpty)
		pm.notEmpty = make(chan struct{})
	}
}

// Replenish ensures at least `required` verified high-speed proxies are available in the pool.
func (pm *ProxyManager) Replenish(ctx context.Context, required int) {
	pm.mu.Lock()
	if len(pm.queue) >= required {
		pm.mu.Unlock()
		return
	}
	if pm.replenishing {
		waitCh := pm.replenishCh
		pm.mu.Unlock()
		select {
		case <-waitCh:
		case <-ctx.Done():
		}
		return
	}
	pm.replenishing = true
	pm.replenishCh = make(chan struct{})
	alreadyLoadedPersisted := pm.persistedLoaded
	pm.mu.Unlock()

	defer func() {
		pm.mu.Lock()
		pm.replenishing = false
		close(pm.replenishCh)
		pm.mu.Unlock()
	}()

	log.Printf("INFO - Replenishing proxies. Aiming for at least %d verified proxies (>= %.2f MB/s against pornhub.com)...",
		required, pm.minSpeedBytesPerSec/(1024*1024))

	// 1. Load and re-validate persisted proxies once per run
	if !alreadyLoadedPersisted {
		persisted := pm.loadPersistedProxies()
		pm.mu.Lock()
		pm.persistedLoaded = true
		for _, p := range persisted {
			pm.testedSet[p] = struct{}{}
		}
		pm.mu.Unlock()

		if len(persisted) > 0 {
			log.Printf("INFO - Re-validating %d persisted proxies against pornhub.com...", len(persisted))
			validPersisted := pm.validateBatch(ctx, persisted)
			pm.enqueueValidated(validPersisted)
			log.Printf("INFO - %d persisted proxies passed validation (>= %.2f MB/s).",
				len(validPersisted), pm.minSpeedBytesPerSec/(1024*1024))
		}
	}

	// 2. Collect from 10 online proxy sources and validate in batches if still short
	if pm.Len() >= required {
		return
	}

	candidates := pm.nextCandidateBatchPool(ctx)
	if len(candidates) == 0 {
		log.Printf("WARNING - No untested proxy candidates remaining across sources.")
		return
	}

	needed := required - pm.Len()
	batchSize := max(needed*25, 400)

	for i := 0; i < len(candidates); i += batchSize {
		if ctx.Err() != nil {
			break
		}
		end := min(i+batchSize, len(candidates))
		batch := candidates[i:end]

		pm.mu.Lock()
		for _, p := range batch {
			pm.testedSet[p] = struct{}{}
		}
		pm.mu.Unlock()

		log.Printf("INFO - Validating batch of %d proxy candidates (%d workers)...", len(batch), pm.workers)
		validNew := pm.validateBatch(ctx, batch)
		pm.enqueueValidated(validNew)

		if len(validNew) > 0 {
			log.Printf("INFO - Found %d high-speed proxies in batch (pool size: %d).", len(validNew), pm.Len())
		}

		if pm.Len() >= required {
			log.Printf("INFO - Reached required number of working proxies (%d).", required)
			break
		}
	}
}

// SaveProxies atomically writes all currently verified working proxies (fastest first) to disk.
func (pm *ProxyManager) SaveProxies() error {
	pm.mu.Lock()
	list := make([]validatedProxy, 0, len(pm.verified))
	for _, v := range pm.verified {
		list = append(list, v)
	}
	filePath := pm.validProxiesFile
	pm.mu.Unlock()

	sort.Slice(list, func(i, j int) bool {
		if list[i].SpeedBps != list[j].SpeedBps {
			return list[i].SpeedBps > list[j].SpeedBps
		}
		return list[i].URL < list[j].URL
	})

	if dir := filepath.Dir(filePath); dir != "" && dir != "." {
		if err := os.MkdirAll(dir, 0755); err != nil {
			return fmt.Errorf("failed creating proxy file directory: %w", err)
		}
	}

	tmpPath := filePath + ".tmp"
	f, err := os.Create(tmpPath)
	if err != nil {
		return fmt.Errorf("failed creating temp proxy file: %w", err)
	}

	bw := bufio.NewWriter(f)
	for _, vp := range list {
		if _, err := bw.WriteString(vp.URL + "\n"); err != nil {
			_ = f.Close()
			_ = os.Remove(tmpPath)
			return err
		}
	}
	if err := bw.Flush(); err != nil {
		_ = f.Close()
		_ = os.Remove(tmpPath)
		return err
	}
	if err := f.Close(); err != nil {
		_ = os.Remove(tmpPath)
		return err
	}
	if err := os.Rename(tmpPath, filePath); err != nil {
		_ = os.Remove(tmpPath)
		return err
	}

	log.Printf("INFO - Saved %d verified working proxies to %s.", len(list), filePath)
	return nil
}

func (pm *ProxyManager) loadPersistedProxies() []string {
	data, err := os.ReadFile(pm.validProxiesFile)
	if err != nil {
		return nil
	}

	seen := make(map[string]struct{})
	var proxies []string
	scanner := bufio.NewScanner(bytes.NewReader(data))
	for scanner.Scan() {
		if norm, ok := normalizeProxyLine(scanner.Text(), "http"); ok {
			if _, dup := seen[norm]; !dup {
				seen[norm] = struct{}{}
				proxies = append(proxies, norm)
			}
		}
	}
	return proxies
}

func (pm *ProxyManager) nextCandidateBatchPool(ctx context.Context) []string {
	pm.mu.Lock()
	var untested []string
	for _, p := range pm.cachedPool {
		if _, tested := pm.testedSet[p]; !tested {
			untested = append(untested, p)
		}
	}
	pm.mu.Unlock()

	if len(untested) > 0 {
		return untested
	}

	fetched := pm.fetchAllSources(ctx)
	pm.mu.Lock()
	pm.cachedPool = fetched
	for _, p := range fetched {
		if _, tested := pm.testedSet[p]; !tested {
			untested = append(untested, p)
		}
	}
	pm.mu.Unlock()
	return untested
}

func (pm *ProxyManager) fetchAllSources(ctx context.Context) []string {
	log.Printf("INFO - Fetching proxies concurrently from %d sources...", len(pm.sources))

	type sourceResult struct {
		proxies []string
	}
	resultsCh := make(chan sourceResult, len(pm.sources))

	var wg sync.WaitGroup
	wg.Add(len(pm.sources))
	for _, src := range pm.sources {
		go func(s ProxySource) {
			defer wg.Done()
			req, err := http.NewRequestWithContext(ctx, http.MethodGet, s.URL, nil)
			if err != nil {
				log.Printf("WARNING - Invalid proxy source %s: %v", s.URL, err)
				return
			}
			resp, err := pm.sourceClient.Do(req)
			if err != nil {
				log.Printf("WARNING - Failed to fetch proxies from %s: %v", s.URL, err)
				return
			}
			defer resp.Body.Close()

			if resp.StatusCode != http.StatusOK {
				log.Printf("WARNING - Proxy source %s returned HTTP %d", s.URL, resp.StatusCode)
				return
			}

			var local []string
			scanner := bufio.NewScanner(resp.Body)
			for scanner.Scan() {
				if norm, ok := normalizeProxyLine(scanner.Text(), s.DefaultScheme); ok {
					local = append(local, norm)
				}
			}
			resultsCh <- sourceResult{proxies: local}
		}(src)
	}

	go func() {
		wg.Wait()
		close(resultsCh)
	}()

	seen := make(map[string]struct{})
	var aggregated []string
	totalRaw := 0

	for res := range resultsCh {
		totalRaw += len(res.proxies)
		for _, p := range res.proxies {
			if _, exists := seen[p]; !exists {
				seen[p] = struct{}{}
				aggregated = append(aggregated, p)
			}
		}
	}

	// Shuffle aggregated pool so batch testing spans across all sources/IP ranges evenly
	rand.Shuffle(len(aggregated), func(i, j int) {
		aggregated[i], aggregated[j] = aggregated[j], aggregated[i]
	})

	log.Printf("INFO - Collected %d proxies (%d unique after deduplication).", totalRaw, len(aggregated))
	return aggregated
}

func (pm *ProxyManager) enqueueValidated(list []validatedProxy) {
	if len(list) == 0 {
		return
	}
	sort.Slice(list, func(i, j int) bool {
		return list[i].SpeedBps > list[j].SpeedBps
	})
	pm.mu.Lock()
	for _, vp := range list {
		pm.verified[vp.URL] = vp
		pm.pushLocked(vp.URL)
	}
	pm.mu.Unlock()
}

func (pm *ProxyManager) validateBatch(ctx context.Context, candidates []string) []validatedProxy {
	if len(candidates) == 0 {
		return nil
	}

	workerCount := min(pm.workers, len(candidates))
	jobs := make(chan string)
	results := make(chan validatedProxy, len(candidates))

	var wg sync.WaitGroup
	wg.Add(workerCount)
	for w := 0; w < workerCount; w++ {
		go func() {
			defer wg.Done()
			discardBuf := make([]byte, 32*1024)
			for proxyURL := range jobs {
				if ctx.Err() != nil {
					return
				}
				if speed, ok := pm.validateProxy(ctx, proxyURL, discardBuf); ok {
					results <- validatedProxy{
						URL:        proxyURL,
						SpeedBps:   speed,
						LastTested: time.Now(),
					}
				}
			}
		}()
	}

	go func() {
		for _, c := range candidates {
			select {
			case jobs <- c:
			case <-ctx.Done():
				close(jobs)
				wg.Wait()
				close(results)
				return
			}
		}
		close(jobs)
		wg.Wait()
		close(results)
	}()

	var valid []validatedProxy
	for vp := range results {
		valid = append(valid, vp)
	}
	return valid
}

func (pm *ProxyManager) validateProxy(ctx context.Context, proxyURL string, discardBuf []byte) (float64, bool) {
	parsedProxy, err := url.Parse(proxyURL)
	if err != nil || parsedProxy.Host == "" {
		return 0, false
	}

	// Stage 1: Fast TCP connectivity pre-check to drop dead IPs before allocating TLS sessions.
	tcpTimeout := min(pm.timeout, 3*time.Second)
	dialer := net.Dialer{Timeout: tcpTimeout}
	conn, err := dialer.DialContext(ctx, "tcp", parsedProxy.Host)
	if err != nil {
		return 0, false
	}
	_ = conn.Close()

	// Stage 2: Full Chrome 120 TLS validation & speed benchmark strictly against pornhub.com.
	timeoutSec := max(int(pm.timeout.Seconds()), 1)
	client, err := tls_client.NewHttpClient(
		tls_client.NewNoopLogger(),
		tls_client.WithTimeoutSeconds(timeoutSec),
		tls_client.WithClientProfile(profiles.Chrome_120),
		tls_client.WithProxyUrl(proxyURL),
	)
	if err != nil {
		return 0, false
	}
	defer client.CloseIdleConnections()

	var (
		totalBytes   int64
		totalBodyDur time.Duration
	)

	for idx, target := range pornhubProbeTargets {
		reqCtx, cancel := context.WithTimeout(ctx, pm.timeout)
		req, reqErr := fhttp.NewRequestWithContext(reqCtx, fhttp.MethodGet, target, nil)
		if reqErr != nil {
			cancel()
			return 0, false
		}

		// Explicitly request uncompressed content so body bytes match actual wire throughput.
		req.Header = fhttp.Header{
			"accept":                    {"text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,*/*;q=0.8"},
			"accept-encoding":           {"identity"},
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
		}

		reqStart := time.Now()
		resp, doErr := client.Do(req)
		if doErr != nil {
			cancel()
			return 0, false
		}

		if resp.StatusCode != http.StatusOK || time.Since(reqStart) > pm.maxLatency {
			_, _ = io.CopyBuffer(io.Discard, resp.Body, discardBuf)
			_ = resp.Body.Close()
			cancel()
			return 0, false
		}

		bodyStart := time.Now()
		if idx == 0 {
			rawBody, readErr := io.ReadAll(io.LimitReader(resp.Body, maxProbeReadBytes))
			bodyDur := time.Since(bodyStart)
			contentEnc := strings.ToLower(resp.Header.Get("Content-Encoding"))
			_ = resp.Body.Close()
			cancel()

			if readErr != nil || len(rawBody) == 0 {
				return 0, false
			}

			htmlText := decodeBodyForInspection(rawBody, contentEnc)
			if strings.Contains(htmlText, "Just a moment") || strings.Contains(htmlText, "Attention Required") {
				return 0, false
			}
			if !strings.Contains(strings.ToLower(htmlText), "pornhub") {
				return 0, false
			}

			totalBytes += int64(len(rawBody))
			totalBodyDur += bodyDur
		} else {
			written, copyErr := io.CopyBuffer(io.Discard, io.LimitReader(resp.Body, maxProbeReadBytes), discardBuf)
			bodyDur := time.Since(bodyStart)
			_ = resp.Body.Close()
			cancel()

			if copyErr != nil || written == 0 {
				break
			}
			totalBytes += written
			totalBodyDur += bodyDur
		}

		if totalBytes >= targetSpeedSampleBytes {
			break
		}
	}

	if totalBytes < minValidSampleBytes || totalBodyDur <= 0 {
		return 0, false
	}

	speedBps := float64(totalBytes) / totalBodyDur.Seconds()
	if speedBps < pm.minSpeedBytesPerSec {
		return speedBps, false
	}
	return speedBps, true
}

func decodeBodyForInspection(raw []byte, contentEncoding string) string {
	if strings.Contains(contentEncoding, "gzip") {
		gz, err := gzip.NewReader(bytes.NewReader(raw))
		if err == nil {
			defer gz.Close()
			if decompressed, readErr := io.ReadAll(io.LimitReader(gz, maxProbeReadBytes)); readErr == nil {
				return string(decompressed)
			}
		}
	}
	return string(raw)
}

func normalizeProxyLine(raw string, defaultScheme string) (string, bool) {
	line := strings.TrimSpace(raw)
	if line == "" || strings.HasPrefix(line, "#") {
		return "", false
	}
	// Strip inline comments or trailing metadata if present in some feeds
	if fields := strings.Fields(line); len(fields) > 0 {
		line = fields[0]
	}

	if !strings.Contains(line, "://") {
		if defaultScheme == "" {
			defaultScheme = "http"
		}
		line = defaultScheme + "://" + line
	}

	u, err := url.Parse(line)
	if err != nil {
		return "", false
	}
	scheme := strings.ToLower(u.Scheme)
	if scheme != "http" && scheme != "https" && scheme != "socks5" {
		return "", false
	}
	host := u.Hostname()
	port := u.Port()
	if host == "" || port == "" {
		return "", false
	}

	normalized := url.URL{
		Scheme: scheme,
		User:   u.User,
		Host:   net.JoinHostPort(host, port),
	}
	return normalized.String(), true
}
