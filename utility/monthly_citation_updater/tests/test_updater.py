import copy
import io
import json
import os
import sys
import unittest
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import Mock, patch
from urllib.error import HTTPError

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
import lambda_function as updater

EXISTING = {"title": "An existing carefully edited research paper", "authors": "A. Author",
            "venue": "Edited conference name", "year": 2025,
            "links": [{"name": "DOI", "url": "https://doi.org/10.1000/existing"}]}
SEED = {"schemaVersion": 1, "updatedAt": "2026-08-01T00:17:00+00:00", "source": updater.SOURCE,
        "papers": [EXISTING]}
RECORD = {"title": EXISTING["title"], "authors": [{"name": "A. Author"}], "year": 2025,
          "venue": "API venue", "externalIds": {"DOI": "10.1000/existing"}}
NOW = "2026-09-01T00:17:00+00:00"


def response(*records, next_offset=None):
    data = {"data": [{"citingPaper": r} for r in records]}
    if next_offset is not None:
        data["next"] = next_offset
    return data


class UpdaterTests(unittest.TestCase):
    def test_unchanged_success_updates_date_and_preserves_curated_metadata(self):
        client = Mock()
        client.get.return_value = response(RECORD)
        actual = updater.refresh(SEED, [], client, NOW)
        self.assertEqual(actual["papers"], [EXISTING])
        self.assertEqual(actual["updatedAt"], NOW)
        self.assertEqual(SEED["updatedAt"], "2026-08-01T00:17:00+00:00")

    def test_partial_source_failure_does_not_mutate_seed(self):
        original = copy.deepcopy(SEED)
        client = Mock()
        client.get.side_effect = [response(RECORD), TimeoutError("source unavailable")]
        with self.assertRaises(TimeoutError):
            updater.refresh(SEED, [], client, NOW)
        self.assertEqual(SEED, original)

    def test_pagination_follows_next_even_on_short_pages(self):
        client = Mock()
        client.get.side_effect = [response(RECORD, next_offset=1), response(RECORD)]
        self.assertEqual(len(updater.citing_papers(client, updater.PAPERS[0])), 2)
        self.assertIn("offset=1", client.get.call_args.args[0])

    def test_broken_pagination_is_not_promoted_as_success(self):
        client = Mock()
        client.get.return_value = response(RECORD, next_offset=0)
        with self.assertRaises(ValueError):
            updater.citing_papers(client, updater.PAPERS[0])

    def test_empty_or_malformed_sources_fail(self):
        for data in [{"message": "rate limited"}, {"data": [None]}, {"data": []}]:
            with self.subTest(data=data):
                client = Mock()
                client.get.return_value = data
                with self.assertRaises(ValueError):
                    updater.refresh(SEED, [], client, NOW)

    def test_deduplicates_new_papers_and_excludes_curated_lab_papers(self):
        new = {**RECORD, "title": "A new external research paper title", "externalIds": {"DOI": "10.1000/new"}}
        lab = {**RECORD, "title": "A manually curated lab paper title", "externalIds": {"DOI": "10.1000/lab"}}
        client = Mock()
        client.get.return_value = response(RECORD, new, lab)
        actual = updater.refresh(SEED, [{"title": lab["title"], "links": []}], client, NOW)
        self.assertEqual(len(actual["papers"]), 2)
        self.assertNotIn(lab["title"], [p["title"] for p in actual["papers"]])

    def test_crossref_failure_does_not_drop_a_valid_citation(self):
        new = {**RECORD, "title": "Another external research paper title", "venue": "", "externalIds": {"DOI": "10.1000/new"}}
        client = Mock()
        client.get.side_effect = [response(new), response(new), response(new), TimeoutError()]
        self.assertEqual(len(updater.refresh(SEED, [], client, NOW)["papers"]), 2)

    def test_non_doi_links_do_not_become_doi_identities(self):
        self.assertEqual(updater.doi_key("https://example.org/paper"), "")
        self.assertEqual(updater.doi_key("https://doi.org/10.1000/ABC"), "10.1000/abc")

    def test_unsafe_snapshot_links_are_rejected(self):
        snapshot = copy.deepcopy(SEED)
        snapshot["papers"][0]["links"][0]["url"] = "javascript:alert(1)"
        with self.assertRaises(ValueError):
            updater.validate_snapshot(snapshot)

    def test_rate_limit_retries_are_bounded(self):
        error = HTTPError("https://api.semanticscholar.org/", 429, "Too many requests", {}, None)
        client = updater.ApiClient()
        with patch.object(client, "pause") as pause, patch("urllib.request.urlopen", side_effect=error) as fetch:
            with self.assertRaises(HTTPError):
                client.get("https://api.semanticscholar.org/")
        self.assertEqual(fetch.call_count, 4)
        self.assertEqual(pause.call_count, 3)

    def test_lambda_only_publishes_after_success_with_conditional_write(self):
        for failure in [False, True]:
            with self.subTest(failure=failure):
                s3 = Mock()
                s3.get_object.return_value = {"Body": io.BytesIO(json.dumps(SEED).encode()), "ETag": '"old"'}
                fresh = {**SEED, "updatedAt": NOW}
                with patch.dict(sys.modules, {"boto3": SimpleNamespace(client=lambda _: s3)}), \
                     patch.dict(os.environ, {"CITATIONS_BUCKET": "test-bucket", "LAB_PAPERS_JSON": "[]"}), \
                     patch.object(updater, "refresh", side_effect=TimeoutError() if failure else None, return_value=fresh):
                    if failure:
                        with self.assertRaises(TimeoutError):
                            updater.lambda_handler({}, None)
                        s3.put_object.assert_not_called()
                    else:
                        updater.lambda_handler({}, None)
                        self.assertEqual(s3.put_object.call_args.kwargs["IfMatch"], '"old"')
                        self.assertEqual(json.loads(s3.put_object.call_args.kwargs["Body"])["updatedAt"], NOW)


if __name__ == "__main__":
    unittest.main()
