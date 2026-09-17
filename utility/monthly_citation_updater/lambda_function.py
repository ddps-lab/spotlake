"""Monthly citation snapshot. External API failures never overwrite the last good data."""

import argparse
import json
import logging
import os
import random
import re
import time
import urllib.error
import urllib.parse
import urllib.request
from datetime import datetime, timezone
from pathlib import Path

LOG = logging.getLogger(__name__)
LOG.setLevel(logging.INFO)
PAPERS = (
    "DOI:10.1109/IISWC55918.2022.00029",
    "arXiv:2202.02973",
    "DOI:10.1145/3543873.3587314",
)
SOURCE = "Semantic Scholar + Crossref"


def title_key(value):
    return re.sub(r"[\W_]+", "", value.casefold())


def doi_key(url):
    match = re.match(r"https?://(?:dx\.)?doi\.org/(10\..+)", url, re.I)
    return urllib.parse.unquote(match[1]).lower() if match else ""


def identities(paper):
    keys = {"title:" + title_key(paper["title"])}
    for link in paper.get("links", []):
        doi = doi_key(link["url"])
        if doi:
            keys.add("doi:" + doi)
    return keys


def validate_snapshot(snapshot):
    if (not isinstance(snapshot, dict) or snapshot.get("schemaVersion") != 1
            or not isinstance(snapshot.get("source"), str)
            or not isinstance(snapshot.get("papers"), list) or not snapshot["papers"]):
        raise ValueError("Invalid or empty citation snapshot")
    updated = snapshot.get("updatedAt")
    if updated is not None:
        if not isinstance(updated, str) or datetime.fromisoformat(updated.replace("Z", "+00:00")).tzinfo is None:
            raise ValueError("Invalid snapshot timestamp")
    for paper in snapshot["papers"]:
        if (not isinstance(paper, dict) or not isinstance(paper.get("title"), str)
                or not paper["title"].strip() or type(paper.get("year")) is not int
                or not isinstance(paper.get("authors"), str)
                or not isinstance(paper.get("venue"), str)
                or not isinstance(paper.get("links"), list)):
            raise ValueError("Invalid publication")
        for link in paper["links"]:
            if (not isinstance(link, dict) or not isinstance(link.get("name"), str)
                    or not isinstance(link.get("url"), str)
                    or urllib.parse.urlsplit(link["url"]).scheme not in ("http", "https")):
                raise ValueError("Invalid publication link")
    return snapshot


class ApiClient:
    def __init__(self, budget_seconds=240):
        self.deadline = time.monotonic() + budget_seconds

    def pause(self, seconds):
        if time.monotonic() + seconds + 1 >= self.deadline:
            raise TimeoutError("Citation refresh time budget exhausted")
        time.sleep(seconds)

    def get(self, url):
        headers = {"User-Agent": "SpotLake citation updater (ddpslab@hanyang.ac.kr)"}
        for attempt in range(4):
            remaining = self.deadline - time.monotonic()
            if remaining < 1:
                raise TimeoutError("Citation refresh time budget exhausted")
            try:
                request = urllib.request.Request(url, headers=headers)
                with urllib.request.urlopen(request, timeout=min(20, remaining)) as response:
                    body = response.read(5_000_001)
                    if len(body) > 5_000_000:
                        raise ValueError("Unexpectedly large API response")
                    return json.loads(body)
            except urllib.error.HTTPError as error:
                if error.code not in (429, 500, 502, 503, 504) or attempt == 3:
                    raise
                delay = min(45, 5 * 3 ** attempt) + random.uniform(0, 2)
                retry_after = error.headers.get("Retry-After", "")
                if retry_after.isdigit():
                    delay = max(delay, int(retry_after))
                LOG.warning("Citation API returned %s; retrying in %.1fs", error.code, delay)
            except (urllib.error.URLError, TimeoutError):
                if attempt == 3:
                    raise
                delay = 5 * 3 ** attempt + random.uniform(0, 2)
            self.pause(delay)


def citing_papers(client, paper_id):
    offset = 0
    found = []
    for _ in range(50):
        query = urllib.parse.urlencode({
            "fields": "title,year,authors,venue,externalIds", "limit": 100, "offset": offset,
        })
        url = "https://api.semanticscholar.org/graph/v1/paper/" + urllib.parse.quote(paper_id, safe="")
        page = client.get(url + "/citations?" + query)
        if not isinstance(page, dict) or not isinstance(page.get("data"), list):
            raise ValueError("Malformed Semantic Scholar response")
        rows = page["data"]
        if any(not isinstance(row, dict) or not isinstance(row.get("citingPaper"), dict) for row in rows):
            raise ValueError("Malformed Semantic Scholar citation entry")
        found.extend(row["citingPaper"] for row in rows)
        next_offset = page.get("next")
        if next_offset is None:
            return found
        if type(next_offset) is not int or next_offset <= offset or not rows:
            raise ValueError("Invalid citation pagination")
        offset = next_offset
        client.pause(1.2)
    raise ValueError("Citation pagination limit reached; refusing a partial update")


def refresh(previous, lab_papers, client, now=None):
    validate_snapshot(previous)
    # All citation sources must finish before we publish anything.
    records = []
    for index, paper_id in enumerate(PAPERS):
        if index:
            client.pause(3)
        records.extend(citing_papers(client, paper_id))
    if not records:
        raise ValueError("All citation sources were empty; keeping previous data")

    lab_keys = set().union(*(identities(p) for p in lab_papers)) if lab_papers else set()
    papers = [p for p in previous["papers"] if not identities(p) & lab_keys]
    known = set().union(*(identities(p) for p in papers)) if papers else set()
    for record in records:
        title = str(record.get("title") or "").strip()
        year = record.get("year")
        authors = [a.get("name", "") for a in (record.get("authors") or []) if isinstance(a, dict)]
        if type(year) is not int or len(title) < 20 or len(title.split()) < 4:
            continue
        # Keep the external-research list separate from the curated lab list.
        if any(title_key(a) in {"kyungyonglee", "kyungkoolee", "kyungalee"} for a in authors):
            continue
        external = record.get("externalIds") or {}
        doi = str(external.get("DOI") or "").strip().lower()
        arxiv = str(external.get("ArXiv") or "").strip()
        links = []
        if doi.startswith("10."):
            links = [{"name": "DOI", "url": "https://doi.org/" + doi}]
        elif re.fullmatch(r"[\w./-]+", arxiv):
            links = [{"name": "ARXIV", "url": "https://arxiv.org/abs/" + arxiv}]
        entry = {"title": title, "authors": ", ".join(authors), "venue": str(record.get("venue") or ""),
                 "year": year, "links": links}
        keys = identities(entry)
        if keys & (lab_keys | known):
            continue
        if not entry["venue"] and doi.startswith("10."):
            try:
                message = client.get("https://api.crossref.org/works/" + urllib.parse.quote(doi, safe=""))["message"]
                entry["venue"] = (message.get("container-title") or [""])[0] or message.get("event", {}).get("name", "")
            except (urllib.error.URLError, TimeoutError, ValueError, KeyError, TypeError):
                # Optional metadata enrichment must not discard a valid citation.
                LOG.warning("Crossref enrichment unavailable for a new citation")
        papers.append(entry)
        known.update(keys)
    papers.sort(key=lambda p: (-p["year"], p["title"].casefold()))
    snapshot = {"schemaVersion": 1, "updatedAt": now or datetime.now(timezone.utc).isoformat(timespec="seconds"),
                "source": SOURCE, "papers": papers}
    return validate_snapshot(snapshot)


def lambda_handler(event, context):
    import boto3  # Included in the Lambda Python runtime; not needed for local dry-runs.

    s3 = boto3.client("s3")
    bucket = os.environ["CITATIONS_BUCKET"]
    key = os.environ.get("CITATIONS_KEY", "citations/citations.json")
    response = s3.get_object(Bucket=bucket, Key=key)
    previous = json.loads(response["Body"].read())
    remaining = context.get_remaining_time_in_millis() / 1000 if context else 300
    snapshot = refresh(previous, json.loads(os.environ["LAB_PAPERS_JSON"]), ApiClient(min(240, remaining - 20)))
    # Conditional write also protects against concurrent manual repairs/deployments.
    s3.put_object(Bucket=bucket, Key=key, Body=(json.dumps(snapshot, ensure_ascii=False) + "\n").encode(),
                  ContentType="application/json; charset=utf-8", CacheControl="public, max-age=3600",
                  IfMatch=response["ETag"])
    LOG.info("Published %s citations; updatedAt=%s", len(snapshot["papers"]), snapshot["updatedAt"])
    return {"papers": len(snapshot["papers"]), "updatedAt": snapshot["updatedAt"]}


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--seed", required=True)
    parser.add_argument("--lab-papers", required=True)
    parser.add_argument("--output", help="Omit to validate/print a summary without writing files")
    args = parser.parse_args()
    result = refresh(json.loads(Path(args.seed).read_text()), json.loads(Path(args.lab_papers).read_text()), ApiClient())
    if args.output:
        Path(args.output).write_text(json.dumps(result, ensure_ascii=False, indent=2) + "\n")
    print(json.dumps({"papers": len(result["papers"]), "updatedAt": result["updatedAt"]}))
