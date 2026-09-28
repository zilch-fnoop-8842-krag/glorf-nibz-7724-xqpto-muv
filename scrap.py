import re
import json
import time
import logging
import random
import html
import threading
from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Optional, List, Dict, Any, Union
from collections import deque
from urllib.parse import urlparse, urljoin

from bs4 import BeautifulSoup
from curl_cffi import requests
from curl_cffi.requests.errors import RequestsError

# Dynamic parser selection for maximum performance
try:
    import lxml
    BS_PARSER = "lxml"
except ImportError:
    BS_PARSER = "html.parser"

logger = logging.getLogger(__name__)

# ==========================================
# Precompiled Regex Constants
# ==========================================
REGEX_USER_INFO = re.compile(r'userInfo|user-info', re.I)
REGEX_HD_BADGE = re.compile(r'hd-tag|title-hd-badge', re.I)
REGEX_QUALITY_HD = re.compile(r'"quality_(1080|720|2160|4k)')
REGEX_IS_VERTICAL = re.compile(r'"isVertical"\s*:\s*true|isVerticalVideo\s*=\s*true', re.I)
REGEX_INFO_PIECE = re.compile(r'infoPiece|info-piece', re.I)
REGEX_SUB_COUNT = re.compile(r'subscribers-count|subCount|followers', re.I)
REGEX_SUB_MATCH = re.compile(r'([\d\.,]+[KkMm]?)')
REGEX_ISO_DURATION = re.compile(r'PT(?:(\d+)H)?(?:(\d+)M)?(?:(\d+)S)?')

# ==========================================
# Custom Exceptions
# ==========================================
class RateLimitException(Exception): pass
class CloudflareBlockException(Exception): pass
class ScrapingException(Exception): pass

# ==========================================
# Data Models
# ==========================================
@dataclass
class Author:
    name: str
    url: Optional[str] = None
    avatar_url: Optional[str] = None
    subscribers: str = "Unknown"
    info: Dict[str, str] = field(default_factory=dict)
    bio: str = "Bio not available"

@dataclass
class VideoMetadata:
    url: str
    video_id: str
    title: str = "Unknown"
    is_vertical: bool = False
    is_hd: bool = False
    duration_seconds: Optional[int] = None
    views: str = "Unknown"
    likes: Dict[str, int] = field(default_factory=lambda: {"up": 0, "down": 0})
    date_iso: Optional[str] = None
    tags: List[str] = field(default_factory=list)
    categories: List[str] = field(default_factory=list)
    image_url: Optional[str] = None
    author: Optional[Author] = None

    def to_dict(self) -> Dict[str, Any]:
        """Convert to a standardized dict matching the format needed by main.py"""
        return {
            "url": self.url,
            "id": self.video_id,
            "title": self.title,
            "is_vertical": self.is_vertical,
            "is_HD": self.is_hd,
            "duration_seconds": self.duration_seconds,
            "views": self.views,
            "likes": self.likes,
            "date_iso": self.date_iso,
            "tags": self.tags,
            "categories": self.categories,
            "image_url": self.image_url,
            "author": {
                "name": self.author.name if self.author else None,
                "avatar_url": self.author.avatar_url if self.author else None,
                "bio": self.author.bio if self.author else None,
                "info": self.author.info if self.author else {},
                "subscribers": self.author.subscribers if self.author else "Unknown"
            }
        }

# ==========================================
# Network Components
# ==========================================
class RateLimiter:
    """Thread-safe rate limiter using a sliding window."""
    def __init__(self, max_requests: int, period_seconds: int):
        self.max_requests = max_requests
        self.period_seconds = period_seconds
        self.timestamps = deque()
        self.lock = threading.Lock()

    def wait_if_needed(self) -> None:
        while True:
            with self.lock:
                now = time.time()
                while self.timestamps and now - self.timestamps[0] >= self.period_seconds:
                    self.timestamps.popleft()
                
                if len(self.timestamps) < self.max_requests:
                    self.timestamps.append(time.time())
                    return
                
                sleep_time = self.period_seconds - (now - self.timestamps[0])
            
            if sleep_time > 0:
                logger.debug(f"Rate limit reached. Sleeping for {sleep_time:.2f}s.")
                time.sleep(sleep_time)

class HTTPClient:
    def __init__(self, rate_limiter: Optional[RateLimiter] = None, max_retries: int = 3, backoff_factor: float = 2.0, proxy: Optional[str] = None):
        self.rate_limiter = rate_limiter
        self.max_retries = max_retries
        self.backoff_factor = backoff_factor
        
        # Configure the proxy cleanly
        if proxy:
            proxy = proxy.strip()
            if proxy.startswith(("socks5://", "http://", "https://")):
                self.proxies = {"http": proxy, "https": proxy}
            elif "://" not in proxy:
                self.proxies = {"http": f"http://{proxy}", "https": f"http://{proxy}"}
            else:
                self.proxies = None
        else:
            self.proxies = None
            
        self.session = None
        self._init_session()

    def _init_session(self) -> None:
        """Initializes or completely rebuilds the session to prevent connection pooling bugs."""
        if self.session:
            try:
                self.session.close()
            except Exception:
                pass
        self.session = requests.Session(impersonate="chrome120", timeout=30, proxies=self.proxies)

    def reset_session(self) -> None:
        """Public method to reset the HTTP session. Clears corrupted/flagged states."""
        self._init_session()

    def get(self, url: str) -> str:
        for attempt in range(self.max_retries + 1):
            if self.rate_limiter:
                self.rate_limiter.wait_if_needed()
            try:
                response = self.session.get(url)
                if response.status_code == 429:
                    raise RateLimitException("HTTP 429 Too Many Requests")
                response.raise_for_status()
                
                if "Just a moment" in response.text or "Attention Required" in response.text:
                    raise CloudflareBlockException("Blocked by Cloudflare Captcha.")
                
                return response.text
                
            except (RateLimitException, RequestsError) as e:
                if attempt == self.max_retries:
                    raise ScrapingException(f"Failed to fetch {url} after {self.max_retries} retries.") from e
                
                sleep_time = self.backoff_factor ** attempt
                logger.warning(f"Request failed ({e}). Retrying in {sleep_time}s...")
                
                # Crucial bug-fix: Destroy the bad session/socket state before the next retry
                self.reset_session()
                time.sleep(sleep_time)
                
            except CloudflareBlockException as e:
                logger.error(str(e))
                # Purge session state on CF block just in case
                self.reset_session()
                raise 

    def close(self) -> None:
        if self.session:
            self.session.close()

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.close()

# ==========================================
# Core Scraper Logic
# ==========================================
class PornhubScraper:
    def __init__(self, http_client: HTTPClient):
        self.client = http_client

    def scrape_videos(self, urls: List[str]) -> List[Optional[VideoMetadata]]:
        """
        Process a list of video URLs flawlessly.
        Clears the session between iterations to prevent "random bogus errors" 
        caused by lingering HTTP/2 connection pooling or flag tracking.
        """
        results = []
        for url in urls:
            try:
                # Resetting the session ensures each URL gets a pristine connection state.
                self.client.reset_session()
                meta = self.scrape_video(url)
                results.append(meta)
            except Exception as e:
                logger.error(f"Unexpected error processing {url}: {e}")
                results.append(None)
        return results

    def scrape_video(self, url: str) -> Optional[VideoMetadata]:
        logger.info(f"Extracting metadata from: {url}")
        try:
            html_content = self.client.get(url)
        except (ScrapingException, CloudflareBlockException) as e:
            logger.error(f"Failed to fetch video page: {e}")
            return None

        soup = BeautifulSoup(html_content, BS_PARSER)
        
        page_title = soup.title.text if soup.title else ""
        if "Just a moment" in page_title or "Attention Required" in page_title:
            logger.error("Blocked by Cloudflare! The IP was challenged.")
            return None
        if "ld+json" not in html_content:
            logger.error("Page loaded, but no video data found. You might be facing an Age Gate or Captcha.")
            return None

        video_id = self._extract_video_id(url)
        meta = VideoMetadata(url=url, video_id=video_id)

        self._parse_ld_json(soup, meta)
        self._parse_dom(soup, html_content, meta)

        if meta.author and meta.author.url:
            self._enrich_author_details(meta.author, video_soup=soup)

        return meta

    def _extract_video_id(self, url: str) -> str:
        parsed = urlparse(url)
        if "viewkey=" in parsed.query:
            match = re.search(r'viewkey=([^&]+)', parsed.query)
            if match:
                return match.group(1)
        # Fallback to random identifier if not found
        return str(random.randint(100000000, 999999999))

    def _parse_ld_json(self, soup: BeautifulSoup, meta: VideoMetadata) -> None:
        ld_json_tag = soup.find("script", type="application/ld+json")
        if not ld_json_tag or not ld_json_tag.string:
            return

        try:
            data = json.loads(ld_json_tag.string)
            
            raw_title = data.get("name")
            if isinstance(raw_title, str):
                meta.title = html.unescape(raw_title)
            elif raw_title:
                meta.title = str(raw_title)
                
            meta.image_url = data.get("thumbnailUrl") or meta.image_url
            
            if data.get("duration"):
                meta.duration_seconds = self._parse_iso_duration(data.get("duration"))
            if data.get("uploadDate"):
                meta.date_iso = self._format_date_iso(data.get("uploadDate"))

            interactions = data.get("interactionStatistic") or []
            if isinstance(interactions, dict):
                interactions = [interactions]
                
            for stat in interactions:
                i_type = stat.get("interactionType")
                if i_type == "http://schema.org/WatchAction":
                    meta.views = str(stat.get("userInteractionCount", meta.views))
                elif i_type == "http://schema.org/LikeAction":
                    likes_count = stat.get("userInteractionCount", 0)
                    try:
                        meta.likes["up"] = int(likes_count)
                    except ValueError:
                        pass

            author_data = data.get("author")
            if isinstance(author_data, str):
                meta.author = Author(name=html.unescape(author_data))
            elif isinstance(author_data, dict):
                raw_name = author_data.get("name")
                if isinstance(raw_name, list) and raw_name:
                    raw_name = raw_name[0]
                
                clean_name = html.unescape(str(raw_name)) if raw_name else "Unknown"
                meta.author = Author(name=clean_name, url=author_data.get("url"))
                
        except (json.JSONDecodeError, TypeError) as e:
            logger.warning(f"Failed to parse ld+json: {e}")

    def _parse_dom(self, soup: BeautifulSoup, raw_html: str, meta: VideoMetadata) -> None:
        if not meta.author:
            meta.author = Author(name="Unknown")
            
        user_info_div = soup.find("div", class_=REGEX_USER_INFO)
        if user_info_div:
            img_tag = user_info_div.find("img")
            if img_tag and img_tag.get("src"):
                meta.author.avatar_url = img_tag["src"]
                
            a_tag = user_info_div.find("a", href=True)
            if a_tag and not meta.author.url:
                href = a_tag.get("href", "")
                if any(x in href for x in ["/model/", "/pornstar/", "/channels/", "/users/"]):
                    meta.author.url = href

        tags_wrapper = soup.find("div", class_="tagsWrapper")
        if tags_wrapper:
            meta.tags = [a.get_text(strip=True) for a in tags_wrapper.find_all("a") 
                         if a.get_text(strip=True) and "Suggest" not in a.get_text(strip=True)]
            
        cats_wrapper = soup.find("div", class_="categoriesWrapper")
        if cats_wrapper:
            meta.categories = [a.get_text(strip=True) for a in cats_wrapper.find_all("a") 
                               if a.get_text(strip=True) and "Suggest" not in a.get_text(strip=True)]

        # Failsafe logic to snatch unparsed upvotes directly from the DOM structure
        if meta.likes["up"] == 0:
            likes_tag = soup.find("span", class_="votesUp")
            if likes_tag:
                rating = likes_tag.get("data-rating") or likes_tag.get_text(strip=True)
                try:
                    meta.likes["up"] = int(rating)
                except ValueError:
                    pass

        if meta.views == "Unknown":
            views_tag = soup.find("span", class_="count")
            if views_tag:
                meta.views = views_tag.get_text(strip=True)

        meta.is_hd = bool(soup.find("span", class_=REGEX_HD_BADGE) or REGEX_QUALITY_HD.search(raw_html))
        meta.is_vertical = bool(REGEX_IS_VERTICAL.search(raw_html))

    def _enrich_author_details(self, author: Author, video_soup: BeautifulSoup) -> None:
        """Securely navigates to the Author's `/about` page to extract deeper intel."""
        if not author.url:
            return

        # SSRF PREVENTION: Strict Validation of domain before executing external requests
        parsed_url = urlparse(author.url)
        if parsed_url.netloc and parsed_url.netloc not in ["www.pornhub.com", "pornhub.com"]:
            return
        
        path = parsed_url.path
        path = re.sub(r'/(videos|photos|playlists|community).*$', '', path).rstrip('/')
        if not path:
            return
            
        about_url = urljoin("https://www.pornhub.com", f"{path}/about")
        
        try:
            html_content = self.client.get(about_url)
            soup = BeautifulSoup(html_content, BS_PARSER)
            
            info_pieces = soup.find_all("div", class_=REGEX_INFO_PIECE)
            for piece in info_pieces:
                text = piece.get_text(separator=" ", strip=True)
                text = re.sub(r'\s+', ' ', text)
                
                if ':' in text:
                    key, val = text.split(':', 1)
                    author.info[key.strip()] = val.strip()
                elif text: 
                    author.info[text.strip()] = ""

            subs_tag = soup.find(class_=REGEX_SUB_COUNT)
            if subs_tag:
                sub_text = subs_tag.get_text(strip=True)
                match = REGEX_SUB_MATCH.search(sub_text)
                if match:
                    author.subscribers = match.group(1)

            bio_tags = soup.select(".aboutMeText, section.aboutMeSection div.text, .bio, .profile-about-me")
            if bio_tags:
                bio_tag = bio_tags[0]
                
                for info in bio_tag.find_all(class_=REGEX_INFO_PIECE):
                    info.decompose()
                
                bio_text = bio_tag.get_text(separator="\n", strip=True)
                if author.name and author.name != "Unknown":
                    bio_text = re.sub(r'(?i)^\s*About\s+' + re.escape(author.name) + r'[\s:-]*', '', bio_text).strip()
                
                if bio_text:
                    author.bio = bio_text
            else:
                meta_desc = soup.find("meta", attrs={"name": "description"})
                if meta_desc and meta_desc.get("content"):
                    author.bio = meta_desc["content"].strip()
                    
        except ScrapingException as e:
            logger.warning(f"Failed to extract deep author details: {e}")

    @staticmethod
    def _parse_iso_duration(iso_str: str) -> Optional[int]:
        if not iso_str: return None
        match = REGEX_ISO_DURATION.match(str(iso_str).upper())
        if not match: return None
            
        h = int(match.group(1) or 0)
        m = int(match.group(2) or 0)
        s = int(match.group(3) or 0)
        return h * 3600 + m * 60 + s

    @staticmethod
    def _format_date_iso(publish_date_raw: Union[datetime, str, None]) -> Optional[str]:
        if not publish_date_raw: return None
        if isinstance(publish_date_raw, str):
            try: 
                dt_obj = datetime.fromisoformat(publish_date_raw.replace("Z", "+00:00"))
            except ValueError: 
                return None
        else: 
            dt_obj = publish_date_raw
                
        if not isinstance(dt_obj, datetime): return None
        if dt_obj.tzinfo is None: dt_obj = dt_obj.replace(tzinfo=timezone.utc)
        return dt_obj.isoformat()
