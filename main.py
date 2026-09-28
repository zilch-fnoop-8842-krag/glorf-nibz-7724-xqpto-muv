#!/usr/bin/env python3
"""
main.py - GitHub Actions video downloader with proxy rotation and lossless chunking.
Reads video URLs from urls.txt, filters out previously downloaded videos, fetches proxies upfront,
dynamically rotates proxies for metadata extraction, assigns the successful proxy for downloading,
splits each video into ≤95 MB binary chunks efficiently, and pushes to the repo.
"""

import os
import sys
import json
import time
import logging
import shutil
import queue
import threading
import subprocess
import zipfile
from pathlib import Path
from datetime import datetime, timezone
from concurrent.futures import ThreadPoolExecutor, as_completed
from typing import List, Dict, Optional, Any, Tuple

import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

# our custom decoupled robust refactored scraper
import scrap

# ------------------------------------------------------------------------------
# Formatting helpers
# ------------------------------------------------------------------------------
def format_length(seconds: Optional[int]) -> str:
    if seconds is None:
        return "Unknown length"
    if not isinstance(seconds, (int, float)):
        return "Invalid length"
    try:
        seconds = int(seconds)
    except (TypeError, ValueError):
        return "Invalid length"
    if seconds < 0:
        return "Invalid length"

    hours = seconds // 3600
    minutes = (seconds % 3600) // 60
    
    h_str = f"{hours} hour{'s' if hours != 1 else ''}"
    m_str = f"{minutes} minute{'s' if minutes != 1 else ''}"
    
    if hours == 0:
        return m_str
    return f"{h_str} and {m_str}"


def format_publish_date(publish_date: Optional[datetime]) -> str:
    if publish_date is None:
        return "Unknown date"
    if not isinstance(publish_date, datetime):
        return "Invalid date"
    if publish_date.tzinfo is None:
        publish_date = publish_date.replace(tzinfo=timezone.utc)

    now = datetime.now(timezone.utc)
    diff = now - publish_date
    if diff.total_seconds() < 0:
        return "just now"

    years = now.year - publish_date.year
    if (now.month, now.day) < (publish_date.month, publish_date.day):
        years -= 1
    months = (now.year - publish_date.year) * 12 + now.month - publish_date.month
    if now.day < publish_date.day:
        months -= 1
    days = diff.days

    if years >= 1:
        return f"{years} year{'s' if years != 1 else ''} ago"
    if months >= 1:
        return f"{months} month{'s' if months != 1 else ''} ago"
    if days >= 1:
        return f"{days} day{'s' if days != 1 else ''} ago"
    if diff.seconds >= 3600:
        hours = diff.seconds // 3600
        return f"{hours} hour{'s' if hours != 1 else ''} ago"
    if diff.seconds >= 60:
        minutes = diff.seconds // 60
        return f"{minutes} minute{'s' if minutes != 1 else ''} ago"
    return "just now"


def download_image(url: Optional[str], output_path: Path, proxy: Optional[str] = None) -> bool:
    if not url:
        return False
    try:
        session = requests.Session()
        retries = Retry(total=3, backoff_factor=0.5, status_forcelist=[500, 502, 503, 504])
        session.mount("https://", HTTPAdapter(max_retries=retries))
        session.mount("http://", HTTPAdapter(max_retries=retries))

        proxies = None
        if proxy:
            proxy_url = proxy if "://" in proxy else f"http://{proxy}"
            proxies = {"http": proxy_url, "https": proxy_url}

        resp = session.get(url, timeout=20, proxies=proxies)
        resp.raise_for_status() 
        with open(output_path, "wb") as f:
            f.write(resp.content)
        return True
    except requests.exceptions.RequestException as e:
        logging.warning("Failed to download image from %s to %s: %s", url, output_path, e)
    except Exception as e:
        logging.warning("Unexpected error downloading image from %s to %s: %s", url, output_path, e)
    return False

# ------------------------------------------------------------------------------
# Highly Optimized Proxy Management
# ------------------------------------------------------------------------------
def validate_proxy(proxy: str, timeout: int, max_latency: float) -> bool:
    """Accepts normal/average speed proxies; rejects extremely slow ones."""
    proxy = proxy.strip()
    if proxy.startswith(("socks5://", "http://", "https://")):
        proxies = {"http": proxy, "https": proxy}
    elif "://" not in proxy:
        proxies = {"http": f"http://{proxy}", "https": f"http://{proxy}"}
    else:
        return False

    try:
        start = time.time()
        # Using checkip.amazonaws.com which is enterprise-grade and avoids ipify's harsh rate-limits
        resp = requests.get("https://checkip.amazonaws.com", proxies=proxies, timeout=timeout)
        elapsed = time.time() - start
        
        text = resp.text.strip()
        # A valid response should contain dots (IPv4) or colons (IPv6) and be very short (avoids captive portals)
        is_valid_ip = ("." in text or ":" in text) and len(text) < 50
        
        return resp.status_code == 200 and is_valid_ip and elapsed <= max_latency
    except Exception:
        return False


def validate_proxies_parallel(candidates: List[str], timeout: int, max_latency: float, max_workers: int = 50) -> List[str]:
    """Scans proxies blazingly fast using a high thread count."""
    valid_proxies = []
    
    def check_one(p):
        if validate_proxy(p, timeout, max_latency):
            return p
        return None

    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        for p in executor.map(check_one, candidates):
            if p:
                valid_proxies.append(p)
                
    return list(dict.fromkeys(valid_proxies))


class ProxyManager:
    """Thread-safe proxy rotation manager with automatic scaling and background replenishment."""
    def __init__(self, config: dict):
        self.config = config
        self.valid_proxies_file = Path(config.get("valid_proxies_file", "valid_proxies.txt"))
        
        # Generous timeouts to accept decent/average speed proxies
        self.timeout = config.get("proxy_validation_timeout", 15)
        self.max_latency = config.get("proxy_max_latency_seconds", 20.0)
        self.workers = config.get("proxy_validation_workers", 50)
        
        self.active_queue = queue.Queue()
        self.lock = threading.Lock()
        self.replenishing = False

    def _fetch_from_sources(self) -> List[str]:
        sources = self.config.get("proxy_sources", [])
        fetched_proxies = []
        session = requests.Session()
        for url in sources:
            try:
                resp = session.get(url, timeout=15)
                if resp.status_code == 200:
                    for line in resp.text.splitlines():
                        cleaned = line.strip()
                        if cleaned and not cleaned.startswith("#"):
                            fetched_proxies.append(cleaned)
            except Exception as e:
                logging.warning("Failed to fetch proxies from %s: %s", url, e)
        return list(set(fetched_proxies))

    def replenish(self, required: int) -> None:
        with self.lock:
            if self.active_queue.qsize() >= required:
                return
            if self.replenishing:
                return
            self.replenishing = True
            
        try:
            logging.info("Replenishing proxies. Aiming for at least %d working proxies...", required)
            valid_proxies = []

            # 1. Load and test persisted proxies first
            if self.valid_proxies_file.exists():
                with open(self.valid_proxies_file, "r", encoding="utf-8") as f:
                    persisted = [line.strip() for line in f if line.strip()]
                if persisted:
                    logging.info("Validating %d persisted proxies...", len(persisted))
                    valid_persisted = validate_proxies_parallel(persisted, self.timeout, self.max_latency, self.workers)
                    for p in valid_persisted:
                        valid_proxies.append(p)
                        self.active_queue.put(p)

            # 2. Fetch new online proxies if we're still short
            needed = required - self.active_queue.qsize()
            if needed > 0:
                logging.info("Still need %d proxies. Fetching from online sources...", needed)
                new_candidates = [p for p in self._fetch_from_sources() if p not in valid_proxies]
                
                if new_candidates:
                    batch_size = max(needed * 4, 150)
                    for i in range(0, len(new_candidates), batch_size):
                        batch = new_candidates[i:i + batch_size]
                        logging.info("Validating batch of %d proxy candidates...", len(batch))
                        
                        valid_new = validate_proxies_parallel(batch, self.timeout, self.max_latency, self.workers)
                        for p in valid_new:
                            valid_proxies.append(p)
                            self.active_queue.put(p)
                            
                        if self.active_queue.qsize() >= required:
                            logging.info("Reached required number of working proxies.")
                            break
                        elif i + batch_size < len(new_candidates):
                            logging.info("Still short of proxies. Testing next batch...")
                else:
                    logging.error("No new proxies could be fetched from any source.")
        finally:
            with self.lock:
                self.replenishing = False

    def get_proxy(self, timeout: int = 5) -> Optional[str]:
        """Requests a proxy from the queue."""
        try:
            return self.active_queue.get(block=True, timeout=timeout)
        except queue.Empty:
            return None

    def report_result(self, proxy: str, success: bool) -> None:
        """Rotates successful proxies back into the active queue for high efficiency."""
        if success:
            self.active_queue.put(proxy)

# ------------------------------------------------------------------------------
# Dynamic Metadata Fetching Wrapper
# ------------------------------------------------------------------------------
def fetch_metadata_with_rotation(
    url: str, 
    proxy_manager: ProxyManager, 
    config: dict
) -> Tuple[Optional[Dict[str, Any]], Optional[str]]:
    """Fetch metadata dynamically rotating proxies. Only proven proxies are recycled."""
    retries = config.get("max_retries_per_video", 3)
    
    for attempt in range(1, retries + 1):
        proxy = proxy_manager.get_proxy(timeout=5)
        if not proxy:
            proxy_manager.replenish(2)
            proxy = proxy_manager.get_proxy(timeout=10)
            if not proxy:
                logging.error("No proxy available to fetch metadata for %s", url)
                return None, None

        logging.info("Metadata attempt %d/%d for %s using proxy %s", attempt, retries, url, proxy)
        
        try:
            with scrap.HTTPClient(proxy=proxy, max_retries=0) as client:
                scraper = scrap.PornhubScraper(http_client=client)
                meta_obj = scraper.scrape_video(url)
                
                if meta_obj:
                    meta = meta_obj.to_dict()
                    logging.info("  '%s' by %s (ID: %s)", meta.get("title"), meta.get("author", {}).get("name"), meta.get("id"))
                    proxy_manager.report_result(proxy, True)
                    return meta, proxy
                else:
                    logging.warning("  Metadata extraction returned None for %s via proxy %s", url, proxy)
                    proxy_manager.report_result(proxy, False)
        except Exception as e:
            logging.warning("  Proxy %s failed to fetch metadata for %s: %s", proxy, url, e)
            proxy_manager.report_result(proxy, False)

    logging.error("Failed to fetch metadata for %s after %d attempts.", url, retries)
    return None, None


def fetch_all_metadata(
    urls: List[str], 
    proxy_manager: ProxyManager, 
    config: dict, 
    max_workers: int = 5
) -> List[Tuple[Dict[str, Any], str]]:
    """Fetch metadata for all videos concurrently, returning the metadata and the proven proxy."""
    results = []
    
    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        future_to_url = {
            executor.submit(fetch_metadata_with_rotation, url, proxy_manager, config): url 
            for url in urls
        }
        for future in as_completed(future_to_url):
            url = future_to_url[future]
            try:
                meta, working_proxy = future.result()
                if meta and meta.get("id"):
                    meta = json.loads(json.dumps(meta, default=str))
                    results.append((meta, working_proxy))
                else:
                    logging.warning("Skipping %s – No metadata or ID returned.", url)
            except Exception as e:
                logging.error("Exception fetching metadata for %s: %s", url, e)
                
    return results

# ------------------------------------------------------------------------------
# Configuration loading
# ------------------------------------------------------------------------------
def load_config(config_path: Path = Path("config.json")) -> dict:
    if not config_path.exists():
        sys.exit(f"Configuration file {config_path} not found. Please create it.")
    with open(config_path, "r", encoding="utf-8") as f:
        return json.load(f)

# ------------------------------------------------------------------------------
# Video downloading and splitting
# ------------------------------------------------------------------------------
def download_video(
    video_url: str,
    video_id: str,
    output_path: Path,
    proxy: str,
    config: dict,
) -> bool:
    if proxy.startswith(("socks5://", "http://", "https://")):
        proxy_arg = proxy
    else:
        proxy_arg = f"http://{proxy}"

    cmd = [
        "yt-dlp",
        video_url,
        "--no-playlist",
        "--impersonate", "Chrome-100",
        "--proxy", proxy_arg,
        "--format", "bestvideo+bestaudio/best",
        "--merge-output-format", "mkv",
        "--output", str(output_path / f"{video_id}.%(ext)s"),
    ]
    
    for key, value in config.get("download_headers", {}).items():
        cmd.extend(["--add-header", f"{key}: {value}"])

    try:
        result = subprocess.run(
            cmd,
            stdout=None,
            stderr=subprocess.PIPE,
            text=True,
            timeout=600,
        )
        if result.returncode != 0:
            logging.error("yt-dlp failed for %s: %s", video_url, result.stderr.strip())
            return False
        return True
    except subprocess.TimeoutExpired:
        logging.error("yt-dlp timed out for %s", video_url)
        return False
    except Exception as e:
        logging.error("Unexpected error during download of %s: %s", video_url, e)
        return False


def find_downloaded_file(download_dir: Path, video_id: str) -> Optional[Path]:
    candidates = [p for p in download_dir.glob(f"{video_id}.*") if p.suffix not in ['.part', '.ytdl']]
    if candidates:
        return candidates[0]
    return None


def split_file(
    file_path: Path,
    video_id: str,
    output_dir: Path,
    chunk_size: int,
) -> List[dict]:
    file_size = file_path.stat().st_size
    if file_size == 0:
        raise ValueError("Empty file, cannot split.")
    
    num_parts = (file_size + chunk_size - 1) // chunk_size
    part_info = []
    
    with open(file_path, "rb") as fin:
        for i in range(1, num_parts + 1):
            zip_filename = f"VID_{video_id}.part{i:03d}.zip"
            zip_path = output_dir / zip_filename
            internal_chunk_name = f"VID_{video_id}.part{i:03d}" 
            
            bytes_read_for_part = 0
            
            try:
                with zipfile.ZipFile(zip_path, "w", zipfile.ZIP_STORED) as zf:
                    with zf.open(internal_chunk_name, "w") as dest:
                        while bytes_read_for_part < chunk_size:
                            block_size = min(chunk_size - bytes_read_for_part, 65536) 
                            data = fin.read(block_size)
                            if not data: 
                                break
                            dest.write(data)
                            bytes_read_for_part += len(data)

            except Exception as e:
                logging.error(f"Failed to create zip file for VID_{video_id}.part{i:03d}: {e}")
                if zip_path.exists():
                    zip_path.unlink()
                raise 
            
            if bytes_read_for_part == 0:
                logging.warning(f"No data read for part {i} of {video_id}, skipping.")
                if zip_path.exists():
                    zip_path.unlink()
                continue

            part_info.append({
                "part": i,
                "name": zip_filename, 
                "size_bytes": zip_path.stat().st_size, 
            })
            
    return part_info


def download_and_split(
    video_url: str,
    metadata: Dict[str, Any],
    proxy_manager: ProxyManager,
    config: dict,
    temp_dir: Path,
    preferred_proxy: Optional[str] = None, 
) -> bool:
    video_id = str(metadata["id"])
    retries = 5  # Hardcoded specifically to enforce the 5 retries limit rule
    success_download = False
    
    folder_name = f"VID_{video_id}"

    # Track proxies used exclusively for THIS video to prevent duplicates
    used_proxies = set()

    for attempt in range(1, retries + 1):
        proxy = None
        if attempt == 1 and preferred_proxy:
            proxy = preferred_proxy
            logging.info("First download attempt for %s using proven metadata proxy %s", video_id, proxy)
        else:
            # We must fetch a brand new proxy that has NOT been used for this video yet.
            pulled_proxies = []
            
            # Try up to 3 replenish rounds to cycle and find a fresh proxy
            for _ in range(3):
                while True:
                    p = proxy_manager.get_proxy(timeout=2)
                    if not p:
                        break  # Queue is currently empty, break to trigger a replenish
                    
                    if p not in used_proxies:
                        proxy = p
                        break
                    else:
                        pulled_proxies.append(p)
                
                if proxy:
                    break
                
                # If we exhausted the queue and still don't have a new proxy,
                # replenish with a larger target to aggressively force fetching new proxies.
                proxy_manager.replenish(10 + len(used_proxies))
                
            # Put back the proxies we pulled that were already used for this video,
            # so other parallel video downloads can still safely use them.
            for pp in pulled_proxies:
                proxy_manager.report_result(pp, True)

            if not proxy:
                logging.error("Unable to obtain a brand new proxy for %s after replenish attempts.", video_id)
                return False

        # Add chosen proxy to the exclude list so it doesn't get used again for this specific video
        used_proxies.add(proxy)

        logging.info("Downloading %s (attempt %d/%d) using proxy %s", video_id, attempt, retries, proxy)
        if download_video(video_url, video_id, temp_dir, proxy, config):
            success_download = True
            proxy_manager.report_result(proxy, True)
            break
        else:
            logging.warning("Download failed for %s with proxy %s", video_id, proxy)
            proxy_manager.report_result(proxy, False)

    if not success_download:
        logging.error("All %d download attempts failed for %s. Skipping.", retries, video_id)
        return False

    downloaded_file = find_downloaded_file(temp_dir, video_id)
    if not downloaded_file:
        logging.error("Downloaded file not found for %s.", video_id)
        return False

    video_folder = Path(folder_name)
    video_folder.mkdir(exist_ok=True)

    thumbnail_filename = "preview.png"
    preview_path = video_folder / thumbnail_filename
    if metadata.get("image_url"):
        thumbnail_downloaded = download_image(metadata["image_url"], preview_path, proxy=proxy)
        metadata["thumbnail_downloaded"] = thumbnail_downloaded
        if thumbnail_downloaded:
            metadata["preview_filename"] = thumbnail_filename
        else:
            metadata["preview_filename"] = None
    else:
        metadata["thumbnail_downloaded"] = False
        metadata["preview_filename"] = None

    avatar_filename = "avatar.png"
    avatar_path = video_folder / avatar_filename
    author_avatar_url = metadata.get("author", {}).get("avatar_url")
    
    if author_avatar_url:
        avatar_downloaded = download_image(author_avatar_url, avatar_path, proxy=proxy)
        metadata["avatar_downloaded"] = avatar_downloaded
        if avatar_downloaded:
            metadata["avatar_filename"] = avatar_filename
        else:
            metadata["avatar_filename"] = None
    else:
        metadata["avatar_downloaded"] = False
        metadata["avatar_filename"] = None

    try:
        chunk_info = split_file(
            downloaded_file,
            video_id,
            video_folder,
            config["chunk_size_bytes"],
        )
    except Exception as e:
        logging.error("Splitting failed for %s: %s", video_id, e)
        if downloaded_file:
            downloaded_file.unlink(missing_ok=True)
        if video_folder.exists():
            shutil.rmtree(video_folder)
        return False
    finally:
        if downloaded_file:
            downloaded_file.unlink(missing_ok=True)

    metadata["chunks"] = chunk_info
    metadata_path = video_folder / "metadata.json"
    with open(metadata_path, "w", encoding="utf-8") as f:
        json.dump(metadata, f, indent=2, ensure_ascii=False)
    return True


# ------------------------------------------------------------------------------
# Git operations
# ------------------------------------------------------------------------------
def run_git_command(*args: str) -> bool:
    cmd = ["git"] + list(args)
    try:
        result = subprocess.run(cmd, capture_output=True, text=True, check=False)
        if result.returncode != 0:
            logging.error("Git command '%s' failed: %s", " ".join(cmd), result.stderr.strip())
            return False
        return True
    except Exception as e:
        logging.error("Failed to run git command: %s", e)
        return False


def commit_and_push() -> bool:
    branch = os.environ.get("GITHUB_REF_NAME", "main")
    repo = os.environ.get("GITHUB_REPOSITORY", "owner/repo")
    logging.info("Committing and pushing to %s on branch %s", repo, branch)

    run_git_command("config", "user.name", "github-actions[bot]")
    run_git_command("config", "user.email", "github-actions[bot]@users.noreply.github.com")

    if not run_git_command("add", "-A"):
        return False

    commit_cmd = ["git", "commit", "-m", "Add video chunks [skip ci]"]
    result = subprocess.run(commit_cmd, capture_output=True, text=True)
    
    if result.returncode != 0:
        output_str = (result.stdout + result.stderr).lower()
        if "nothing to commit" in output_str:
            logging.info("Nothing to commit. Repository is up-to-date.")
            return True 
        logging.error("Commit failed: %s", result.stderr.strip())
        return False

    if not run_git_command("push", "origin", f"HEAD:{branch}"):
        return False
    return True


# ------------------------------------------------------------------------------
# Main execution
# ------------------------------------------------------------------------------
def main():
    logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
    
    config = load_config()
    urls_file = Path(config.get("urls_file", "urls.txt"))
    temp_dir = Path(config.get("temp_dir", "temp_downloads"))
    temp_dir.mkdir(exist_ok=True)
    index_path = Path("index.json")

    # ---------------------------------------------------
    # 1. Load index & Read URLs
    # ---------------------------------------------------
    existing_index = []
    existing_urls = set()
    if index_path.exists():
        with open(index_path, "r", encoding="utf-8") as f:
            try:
                existing_index = json.load(f)
                for item in existing_index:
                    meta_url = item.get("metadata", {}).get("url")
                    if meta_url:
                        existing_urls.add(meta_url)
            except json.JSONDecodeError:
                logging.warning("Existing index.json is corrupted or empty. Starting with an empty index.")
    
    if not urls_file.exists():
        sys.exit(f"URLs file '{urls_file}' not found.")
    
    with open(urls_file, "r", encoding="utf-8") as f:
        raw_video_urls = list(dict.fromkeys([line.strip() for line in f if line.strip()]))
        
    video_urls = [u for u in raw_video_urls if u not in existing_urls]
    
    if not video_urls:
        logging.info("No new URLs to process. Exiting.")
        return

    # ---------------------------------------------------
    # 2. Acquire Proxies
    # ---------------------------------------------------
    proxy_manager = ProxyManager(config)
    required_proxies = len(video_urls) * 2
    
    logging.info("Acquiring working proxies for %d new videos upfront...", len(video_urls))
    proxy_manager.replenish(required_proxies)
    
    if proxy_manager.active_queue.empty():
        sys.exit("No proxies available. Aborting.")

    # ---------------------------------------------------
    # 3. Fetch Metadata Using Dynamic Proxy Rotation
    # ---------------------------------------------------
    logging.info("Fetching metadata for %d videos dynamically rotating through proxies...", len(video_urls))
    videos_metadata_tuples = fetch_all_metadata(
        video_urls, 
        proxy_manager, 
        config, 
        max_workers=config.get("concurrent_downloads", 5)
    )
    
    if not videos_metadata_tuples:
        sys.exit("Failed to fetch metadata for any video. Exiting.")

    # ---------------------------------------------------
    # 4. Concurrent Download & Split
    # ---------------------------------------------------
    concurrent_workers = config.get("concurrent_downloads", 5)
    logging.info("Starting concurrent downloads processing with %d workers.", concurrent_workers)
    success_ids = []

    with ThreadPoolExecutor(max_workers=concurrent_workers) as executor:
        future_to_video = {}
        for meta, assigned_proxy in videos_metadata_tuples:
            if any(item.get('video_id') == meta['id'] for item in existing_index):
                logging.info("Video %s already exists in index.json, skipping download.", meta['id'])
                continue

            future = executor.submit(
                download_and_split,
                meta["url"],
                meta,
                proxy_manager,
                config,
                temp_dir,
                assigned_proxy
            )
            future_to_video[future] = meta["id"]

        for future in as_completed(future_to_video):
            vid = future_to_video[future]
            try:
                if future.result():
                    success_ids.append(vid)
                else:
                    logging.error("Processing failed for video %s.", vid)
            except Exception as e:
                logging.error("Unhandled exception for video %s: %s", vid, e)

    if not success_ids:
        logging.info("No new videos processed successfully. Cleaning up.")
        shutil.rmtree(temp_dir, ignore_errors=True)
        return

    # ---------------------------------------------------
    # 5. Generate index.json
    # ---------------------------------------------------
    repo = os.environ.get("GITHUB_REPOSITORY", "owner/repo")
    branch = os.environ.get("GITHUB_REF_NAME", "main")
    
    no_preview_url = config.get("no_preview_image_url", "https://example.com/no-preview.png")
    no_avatar_url = config.get("no_avatar_image_url", "https://example.com/no-avatar.png")

    new_entries = []
    for meta, _ in videos_metadata_tuples:
        if meta["id"] not in success_ids: continue
        
        folder_name = f"VID_{meta['id']}"

        chunks_with_urls = [{
            "part": c["part"],
            "url": f"https://raw.githubusercontent.com/{repo}/{branch}/{folder_name}/{c['name']}",
            "size_bytes": c["size_bytes"],
        } for c in meta.get("chunks", [])]

        final_preview_url = no_preview_url
        if meta.get("thumbnail_downloaded") and meta.get("preview_filename"):
            final_preview_url = f"https://raw.githubusercontent.com/{repo}/{branch}/{folder_name}/{meta['preview_filename']}"

        final_avatar_url = no_avatar_url
        if meta.get("avatar_downloaded") and meta.get("avatar_filename"):
            final_avatar_url = f"https://raw.githubusercontent.com/{repo}/{branch}/{folder_name}/{meta['avatar_filename']}"
        
        index_meta = meta.copy()
        index_meta["image_url"] = final_preview_url  
        
        if index_meta.get("author"):
            index_meta["author"]["avatar_url"] = final_avatar_url

        pub_date = None
        if index_meta.get("date_iso"):
            try:
                pub_date = datetime.fromisoformat(index_meta["date_iso"])
            except Exception as e:
                logging.warning("Could not parse publish date '%s' for video %s: %s", index_meta['date_iso'], index_meta['id'], e)

        new_entries.append({
            "video_id": index_meta["id"],
            "title": index_meta.get("title", ""),
            "author": index_meta.get("author", {}).get("name", ""),
            "duration_display": format_length(index_meta.get("duration_seconds")),
            "publish_date_display": format_publish_date(pub_date),
            "preview_url": final_preview_url, 
            "downloaded_already": False,      
            "deleted": False,                 
            "chunks": chunks_with_urls,
            "metadata": index_meta,         
        })

    final_index = existing_index + new_entries
    with open(index_path, "w", encoding="utf-8") as f:
        json.dump(final_index, f, indent=2, ensure_ascii=False)
    logging.info("Generated index.json with %d entries (newly added: %d).", len(final_index), len(new_entries))

    # ---------------------------------------------------
    # 6. Commit and push
    # ---------------------------------------------------
    if not commit_and_push():
        sys.exit("Git push failed.")

    shutil.rmtree(temp_dir, ignore_errors=True)
    logging.info("All done.")

if __name__ == "__main__":
    main()
