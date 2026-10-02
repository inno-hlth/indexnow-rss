#!/usr/bin/env python3
"""
IndexNow RSS submitter

Reads one or more RSS/Atom feeds per site, finds articles that are new (or
updated) since the last run, and submits them to IndexNow using that site's
own key. Supports any number of sites, each with its own key and feeds.

Standard library only - no pip install needed. Python 3.8+.

Usage:
    python indexnow_rss.py                      # run all sites
    python indexnow_rss.py --site site-one      # run one site
    python indexnow_rss.py --dry-run -v         # show what would be sent
    python indexnow_rss.py --generate-key       # make a new IndexNow key
"""

import argparse
import json
import logging
import os
import secrets
import sys
import time
import urllib.error
import urllib.request
import xml.etree.ElementTree as ET
from datetime import datetime, timedelta, timezone
from email.utils import parsedate_to_datetime
from pathlib import Path
from urllib.parse import urlparse

DEFAULTS = {
    "endpoint": "https://api.indexnow.org/indexnow",
    "max_age_hours": 48,       # ignore feed items published longer ago than this
    "resubmit_updates": True,  # resubmit a URL when its feed date moves forward
    "retain_days": 30,         # forget submitted URLs no longer in the feed after this
    "timeout": 30,
    "retries": 3,
}
BATCH_SIZE = 10_000  # IndexNow maximum per request
USER_AGENT = "IndexNow-RSS-Submitter/1.0"
DATE_TAGS = {"pubDate", "published", "updated", "date", "modified"}

log = logging.getLogger("indexnow")


# --------------------------------------------------------------------------- #
# Helpers
# --------------------------------------------------------------------------- #
def now_utc():
    return datetime.now(timezone.utc)


def parse_date(text):
    """Parse RFC 822 (RSS) or ISO 8601 (Atom) dates into aware UTC datetimes."""
    if not text:
        return None
    text = text.strip()
    dt = None
    try:
        dt = parsedate_to_datetime(text)
    except (TypeError, ValueError, IndexError):
        pass
    if dt is None:
        try:
            dt = datetime.fromisoformat(text.replace("Z", "+00:00"))
        except ValueError:
            return None
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    return dt.astimezone(timezone.utc)


def local_name(tag):
    return tag.rsplit("}", 1)[-1] if isinstance(tag, str) else ""


def with_retries(fn, retries, what):
    last = None
    for attempt in range(1, retries + 1):
        try:
            return fn()
        except (urllib.error.URLError, TimeoutError, ConnectionError) as e:
            # HTTP 4xx errors won't improve on retry
            if isinstance(e, urllib.error.HTTPError) and 400 <= e.code < 500:
                raise
            last = e
            if attempt < retries:
                wait = 2 ** attempt
                log.warning("%s failed (%s), retrying in %ss", what, e, wait)
                time.sleep(wait)
    raise last


# --------------------------------------------------------------------------- #
# Feed fetching and parsing
# --------------------------------------------------------------------------- #
def fetch(url, timeout, retries):
    def go():
        req = urllib.request.Request(url, headers={
            "User-Agent": USER_AGENT,
            "Accept": "application/rss+xml, application/atom+xml, "
                      "application/xml;q=0.9, */*;q=0.8",
        })
        with urllib.request.urlopen(req, timeout=timeout) as r:
            return r.read()
    return with_retries(go, retries, f"Fetching {url}")


def parse_feed(xml_bytes):
    """Return [(url, datetime_or_None)] for every item/entry in an RSS or Atom feed."""
    root = ET.fromstring(xml_bytes)
    results = []
    for el in root.iter():
        if local_name(el.tag) not in ("item", "entry"):
            continue
        link, guid, dates = None, None, []
        for child in el:
            name = local_name(child.tag)
            text = (child.text or "").strip()
            if name == "link":
                href = child.get("href")
                if href:  # Atom style
                    if child.get("rel", "alternate") == "alternate" and not link:
                        link = href.strip()
                elif text and not link:  # RSS style
                    link = text
            elif name == "guid" and text:
                if child.get("isPermaLink", "true").lower() == "true":
                    guid = text
            elif name in DATE_TAGS:
                d = parse_date(text)
                if d:
                    dates.append(d)
        url = link or (guid if guid and guid.startswith("http") else None)
        if url:
            results.append((url, max(dates) if dates else None))
    return results


# --------------------------------------------------------------------------- #
# State (what has already been submitted, per site)
# --------------------------------------------------------------------------- #
def load_state(path):
    if not path.exists():
        return None
    try:
        return json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError) as e:
        log.error("Could not read state file %s (%s); treating as first run", path, e)
        return None


def save_state(path, state):
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp = path.with_suffix(".tmp")
    tmp.write_text(json.dumps(state, indent=2, sort_keys=True), encoding="utf-8")
    tmp.replace(path)


# --------------------------------------------------------------------------- #
# IndexNow submission
# --------------------------------------------------------------------------- #
RESPONSE_HINTS = {
    200: "OK",
    202: "Accepted (key validation pending)",
    400: "Bad request - invalid format",
    403: "Forbidden - key not valid or key file not found at keyLocation",
    422: "Unprocessable - URLs don't belong to host, or key mismatch",
    429: "Too many requests - slow down",
}


def submit(endpoint, host, key, key_location, urls, timeout, retries):
    body = json.dumps({
        "host": host,
        "key": key,
        "keyLocation": key_location,
        "urlList": urls,
    }).encode("utf-8")

    def go():
        req = urllib.request.Request(endpoint, data=body, method="POST", headers={
            "Content-Type": "application/json; charset=utf-8",
            "User-Agent": USER_AGENT,
        })
        try:
            with urllib.request.urlopen(req, timeout=timeout) as r:
                return r.status, r.read().decode("utf-8", "replace")
        except urllib.error.HTTPError as e:
            if e.code >= 500 or e.code == 429:
                raise urllib.error.URLError(f"HTTP {e.code}")  # retryable
            return e.code, e.read().decode("utf-8", "replace")

    return with_retries(go, retries, f"Submitting to {endpoint}")


# --------------------------------------------------------------------------- #
# Per-site run
# --------------------------------------------------------------------------- #
def resolve_key(site):
    if site.get("key_env"):
        key = os.environ.get(site["key_env"], "").strip()
        if not key:
            raise ValueError(f"environment variable {site['key_env']} is empty")
        return key
    key = str(site.get("key", "")).strip()
    if not key:
        raise ValueError("no 'key' or 'key_env' set")
    return key


def run_site(site, settings, state_dir, dry_run):
    name = site["name"]
    host = site["host"].strip().lower()
    key = resolve_key(site)
    key_location = site.get("key_location") or f"https://{host}/{key}.txt"
    feeds = site.get("feeds") or ([site["feed"]] if site.get("feed") else [])
    if not feeds:
        raise ValueError("no 'feeds' listed")

    state_path = state_dir / f"{name}.json"
    state = load_state(state_path)
    first_run = state is None
    submitted = (state or {}).get("submitted", {})

    cutoff = now_utc() - timedelta(hours=settings["max_age_hours"])
    now_iso = now_utc().isoformat()

    # Collect items from all feeds, de-duplicated by URL (keep newest date)
    items = {}
    feed_errors = 0
    for feed_url in feeds:
        try:
            entries = parse_feed(fetch(feed_url, settings["timeout"], settings["retries"]))
            log.info("[%s] %s: %d items", name, feed_url, len(entries))
        except Exception as e:  # noqa: BLE001 - keep other feeds going
            log.error("[%s] could not read %s: %s", name, feed_url, e)
            feed_errors += 1
            continue
        for url, dt in entries:
            if url not in items or (dt and (items[url] is None or dt > items[url])):
                items[url] = dt

    # Decide what to send
    to_send, skipped_host, skipped_old = [], 0, 0
    for url, dt in items.items():
        if (urlparse(url).hostname or "").lower() != host:
            skipped_host += 1
            log.debug("[%s] skip (different host): %s", name, url)
            continue
        prev = submitted.get(url)
        if prev is None:
            if dt is not None and dt < cutoff:
                skipped_old += 1
                continue
            to_send.append(url)
        elif settings["resubmit_updates"] and dt is not None:
            prev_dt = parse_date(prev.get("date"))
            if prev_dt is None or dt > prev_dt:
                to_send.append(url)

    if skipped_host:
        log.warning("[%s] skipped %d URL(s) not on host %s", name, skipped_host, host)
    if skipped_old:
        log.info("[%s] skipped %d item(s) older than %dh", name,
                 skipped_old, settings["max_age_hours"])
    log.info("[%s] %d URL(s) to submit%s", name, len(to_send),
             " (first run)" if first_run else "")

    ok = feed_errors == 0
    sent = []
    for i in range(0, len(to_send), BATCH_SIZE):
        batch = to_send[i:i + BATCH_SIZE]
        if dry_run:
            for u in batch:
                log.info("[%s] DRY RUN would submit: %s", name, u)
            continue
        try:
            status, text = submit(settings["endpoint"], host, key, key_location,
                                  batch, settings["timeout"], settings["retries"])
        except Exception as e:  # noqa: BLE001
            log.error("[%s] submission failed: %s", name, e)
            ok = False
            break
        hint = RESPONSE_HINTS.get(status, "")
        if status in (200, 202):
            log.info("[%s] submitted %d URL(s): HTTP %s %s", name, len(batch), status, hint)
            sent.extend(batch)
        else:
            log.error("[%s] HTTP %s %s %s", name, status, hint, text[:300])
            ok = False
            break

    if dry_run:
        return ok

    # Record what was sent, then prune old entries no longer in the feed
    for url in sent:
        dt = items.get(url)
        submitted[url] = {"date": dt.isoformat() if dt else None, "submitted_at": now_iso}
    retain_cutoff = now_utc() - timedelta(days=settings["retain_days"])
    for url in list(submitted):
        if url in items:
            continue
        ts = parse_date(submitted[url].get("submitted_at"))
        if ts and ts < retain_cutoff:
            del submitted[url]

    # No run timestamp is stored, so the file (and the repo) only changes
    # when something was actually submitted or pruned.
    save_state(state_path, {"site": name, "host": host, "submitted": submitted})
    return ok


# --------------------------------------------------------------------------- #
# Entry point
# --------------------------------------------------------------------------- #
def main():
    p = argparse.ArgumentParser(description="Submit new RSS items to IndexNow.")
    p.add_argument("--config", default="config.json")
    p.add_argument("--state-dir", default="state")
    p.add_argument("--site", action="append", help="Only run this site (repeatable)")
    p.add_argument("--dry-run", action="store_true", help="Show what would be sent; change nothing")
    p.add_argument("-v", "--verbose", action="store_true")
    p.add_argument("--generate-key", action="store_true", help="Print a new IndexNow key and exit")
    args = p.parse_args()

    logging.basicConfig(level=logging.DEBUG if args.verbose else logging.INFO,
                        format="%(asctime)s %(levelname)s %(message)s",
                        datefmt="%Y-%m-%d %H:%M:%S")

    if args.generate_key:
        print(secrets.token_hex(16))
        return 0

    try:
        config = json.loads(Path(args.config).read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError) as e:
        log.error("Could not read config %s: %s", args.config, e)
        return 2

    settings = {**DEFAULTS, **config.get("defaults", {})}
    sites = config.get("sites", [])
    if args.site:
        sites = [s for s in sites if s.get("name") in args.site]
        if not sites:
            log.error("No matching sites for --site %s", args.site)
            return 2

    state_dir = Path(args.state_dir)
    failures = 0
    for site in sites:
        site_settings = {**settings, **{k: v for k, v in site.items() if k in DEFAULTS}}
        try:
            if not run_site(site, site_settings, state_dir, args.dry_run):
                failures += 1
        except Exception as e:  # noqa: BLE001 - one bad site shouldn't stop the rest
            log.error("[%s] %s", site.get("name", "?"), e)
            failures += 1

    if failures:
        log.error("%d of %d site(s) had problems", failures, len(sites))
        return 1
    return 0


if __name__ == "__main__":
    sys.exit(main())
