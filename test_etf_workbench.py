import unittest
from datetime import date
import base64
import gzip
import json
import sys
import types

import pandas as pd

# Pure calculation tests do not call AkShare; allow them to run in a minimal local runtime.
sys.modules.setdefault("akshare", types.ModuleType("akshare"))
sys.modules.setdefault("requests", types.ModuleType("requests"))
import etf_workbench as workbench


class MarketDataTests(unittest.TestCase):
    def setUp(self):
        self.history = pd.DataFrame(
            {
                "date": pd.to_datetime(["2026-08-07", "2026-08-10"]),
                "close": [1.00, 1.05],
            }
        )

    def test_live_quote_on_new_day_is_appended(self):
        merged = workbench._merge_live_price(self.history, 1.10, date(2026, 8, 11))
        self.assertEqual(len(merged), 3)
        self.assertEqual(merged.iloc[-2]["close"], 1.05)
        self.assertEqual(merged.iloc[-1]["close"], 1.10)

    def test_same_day_live_quote_replaces_terminal_value(self):
        merged = workbench._merge_live_price(self.history, 1.08, date(2026, 8, 10))
        self.assertEqual(len(merged), 2)
        self.assertEqual(merged.iloc[-1]["close"], 1.08)

    def test_wilson_bound_penalizes_small_samples(self):
        self.assertLess(
            workbench._wilson_lower_bound(6, 8),
            workbench._wilson_lower_bound(60, 80),
        )

    def test_composite_score_rewards_broad_confirmation(self):
        base = {
            "daily_volatility20": 2.0,
            "drawdown": -2.0,
            "evidence_quality": 60.0,
            "above_ma20": True,
            "above_ma60": True,
            "rs20": 1.0,
            "rs60": 1.0,
        }
        broad = {**base, "score": 8.0, "rs_score": 6.0, "burst_score": 2.0}
        spike = {
            **base,
            "score": 1.0,
            "rs_score": -1.0,
            "burst_score": 8.0,
            "above_ma60": False,
            "rs20": -1.0,
            "rs60": -1.0,
        }
        rows = [broad, spike]
        workbench._calculate_composite_scores(rows)
        self.assertGreater(broad["composite_score"], spike["composite_score"])

    def test_dashboard_embeds_compressed_fund_catalog(self):
        context = {
            "now": workbench.datetime.now(workbench.BEIJING_TZ),
            "mode": "morning", "dynamic_count": 0, "watch_count": 0,
            "results": [], "failures": [],
            "fund_catalog": [{"code": "017937", "name": "易方达中证医疗ETF联接发起式A"}],
        }
        page = workbench.build_dashboard(context, "test")
        marker = '"fund_catalog_gzip": "'
        encoded = page.split(marker, 1)[1].split('"', 1)[0]
        decoded = json.loads(gzip.decompress(base64.b64decode(encoded)))
        self.assertEqual(decoded, context["fund_catalog"])


if __name__ == "__main__":
    unittest.main()
