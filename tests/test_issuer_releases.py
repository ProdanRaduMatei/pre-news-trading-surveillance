from __future__ import annotations

import sys
import tempfile
import unittest
from argparse import Namespace
from dataclasses import replace
from pathlib import Path
from unittest.mock import patch

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))

from pre_news_trading_surveillance import db  # noqa: E402
from pre_news_trading_surveillance.cli import cmd_ingest_press_releases  # noqa: E402
from pre_news_trading_surveillance.ingest import issuer_releases  # noqa: E402
from pre_news_trading_surveillance.settings import default_paths  # noqa: E402


RSS_SAMPLE = """\
<rss version="2.0">
  <channel>
    <title>Issuer Newsroom</title>
    <item>
      <title>Apple Reports First Quarter Results</title>
      <link>https://example.com/apple-q1-results</link>
      <guid>apple-q1-results</guid>
      <pubDate>Tue, 30 Jan 2024 21:30:00 GMT</pubDate>
      <description><![CDATA[Apple today announced financial results for its first quarter.]]></description>
    </item>
    <item>
      <title>Apple Reaffirms Outlook</title>
      <link>https://example.com/apple-outlook</link>
      <guid>apple-outlook</guid>
      <pubDate>Wed, 31 Jan 2024 12:00:00 GMT</pubDate>
      <description>Apple reaffirmed its long-term outlook.</description>
    </item>
  </channel>
</rss>
"""


class IssuerReleaseTests(unittest.TestCase):
    def test_parse_feed_releases_handles_rss_items(self) -> None:
        feed = issuer_releases.IssuerFeedConfig(
            ticker="AAPL",
            issuer_name="Apple Inc.",
            feed_url="https://example.com/apple.rss",
            source_name="Apple Newsroom",
            official_homepage="https://example.com",
        )

        releases = issuer_releases.parse_feed_releases(
            RSS_SAMPLE,
            feed=feed,
            raw_path=Path("/tmp/apple.xml"),
            per_feed_limit=5,
            ingested_at="2024-02-01T00:00:00+00:00",
        )

        self.assertEqual(len(releases), 2)
        self.assertEqual(releases[0].ticker, "AAPL")
        self.assertEqual(releases[0].published_at, "2024-01-30T21:30:00+00:00")
        self.assertIn("financial results", releases[0].summary_text or "")
        self.assertTrue(releases[0].release_id.startswith("issuer-release:AAPL:"))

    def test_cmd_ingest_press_releases_persists_rows_and_snapshots(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            root = Path(tmpdir)
            paths = replace(default_paths(root=root), sql_dir=Path(__file__).resolve().parents[1] / "sql")
            paths.ensure_directories()
            config_path = root / "issuer_feeds.toml"
            config_path.write_text(
                """
[[feeds]]
ticker = "AAPL"
issuer_name = "Apple Inc."
source_name = "Apple Newsroom"
feed_url = "https://example.com/apple.rss"
official_homepage = "https://example.com"
                """.strip(),
                encoding="utf-8",
            )

            args = Namespace(
                config=config_path,
                tickers=["AAPL"],
                per_feed_limit=10,
                user_agent="Unit Test Agent/1.0",
                timeout_seconds=30,
                skip_db=False,
                parent_run_id=None,
            )

            with patch("pre_news_trading_surveillance.cli.default_paths", return_value=paths):
                with patch(
                    "pre_news_trading_surveillance.ingest.issuer_releases.fetch_feed_xml",
                    return_value=RSS_SAMPLE,
                ):
                    exit_code = cmd_ingest_press_releases(args)

            self.assertEqual(exit_code, 0)
            releases = db.load_raw_issuer_releases(paths.db_path, ticker="AAPL")
            self.assertEqual(len(releases), 2)

            raw_snapshots = list((paths.raw_dir / "issuer_releases").glob("AAPL_*.xml"))
            self.assertEqual(len(raw_snapshots), 1)
            bronze_snapshots = list((paths.bronze_dir / "issuer_releases").glob("issuer_releases_*.ndjson"))
            self.assertEqual(len(bronze_snapshots), 1)

            run = db.list_ingestion_runs(paths.db_path, limit=1, pipeline_name="issuer_press_releases")[0]

        self.assertEqual(run["status"], "success")
        self.assertEqual(run["row_count"], 2)
        self.assertEqual(run["metadata"]["tickers"], ["AAPL"])
        self.assertIn(str(raw_snapshots[0]), run["artifact_paths"])
        self.assertIn(str(bronze_snapshots[0]), run["artifact_paths"])


if __name__ == "__main__":
    unittest.main()
