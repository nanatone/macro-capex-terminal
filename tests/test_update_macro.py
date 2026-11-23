import copy
import importlib.util
import json
from pathlib import Path
import unittest
from unittest.mock import patch
import pandas as pd

ROOT = Path(__file__).resolve().parents[1]
spec = importlib.util.spec_from_file_location("update_macro", ROOT / "scripts/update_macro.py")
macro = importlib.util.module_from_spec(spec)
spec.loader.exec_module(macro)


class MacroDataTests(unittest.TestCase):
    def test_weekly_percentile_uses_three_calendar_years(self):
        dates = pd.date_range("2022-01-01", periods=210, freq="W")
        values = pd.Series([100.0] * 53 + [0.0] * 156 + [1.0], index=dates)
        self.assertEqual(macro.calc_stats(values)["percentile_3y"], 99)

    def test_fred_tga_and_wresbal_are_converted_to_billions_and_dated(self):
        data = json.loads((ROOT / "data/macro_daily.json").read_text(encoding="utf-8"))

        class FakeFred:
            def __init__(self, **kwargs):
                pass

            def get_series(self, code, **kwargs):
                idx = pd.to_datetime(["2026-10-21", "2026-10-28"])
                if code == "WTREGEN":
                    return pd.Series([950000.0, 999727.0], index=idx)
                if code == "WRESBAL":
                    return pd.Series([2962607.0, 2893770.0], index=idx)
                return pd.Series([4.0, 5.0], index=idx)

        with patch.object(macro, "Fred", FakeFred), patch.dict("os.environ", {"FRED_API_KEY": "test-only"}):
            result = macro.fetch_us_macro(copy.deepcopy(data["metrics"]["us"]))
        self.assertEqual(result["tga"]["value"], 999.73)
        self.assertEqual(result["tga"]["observation_date"], "2026-10-28")
        self.assertEqual(result["wresbal"]["value"], 2893.77)
        self.assertEqual(result["wresbal"]["observation_date"], "2026-10-28")

    def test_total_fred_failure_does_not_claim_update_success(self):
        data = json.loads((ROOT / "data/macro_daily.json").read_text(encoding="utf-8"))

        class BrokenFred:
            def __init__(self, **kwargs):
                pass

            def get_series(self, *args, **kwargs):
                raise RuntimeError("offline")

        with patch.object(macro, "Fred", BrokenFred), patch.dict("os.environ", {"FRED_API_KEY": "test-only"}):
            with self.assertRaises(RuntimeError):
                macro.fetch_us_macro(data["metrics"]["us"])

    def test_rule_engine_and_soft_reports_are_consistent(self):
        us = copy.deepcopy(macro.DEFAULT_US_METRICS)
        cn = copy.deepcopy(macro.DEFAULT_CN_METRICS)
        engine_out = macro.run_hard_rule_engine(us, cn)
        self.assertEqual(len(engine_out["hard_rules"]), 7)
        self.assertEqual(
            engine_out["barbell_allocation"]["defense_total"] + engine_out["barbell_allocation"]["offense_total"],
            100
        )
        reports = macro.generate_soft_reports(us, cn, engine_out)
        self.assertIn("边际回落", reports["trading_perspective"]["transmission_logic"])
        self.assertIn("R007-DR007", reports["wealth_perspective"]["core_marginal_change"])

    def test_eight_dim_commodity_grading_and_five_bucket_barbell(self):
        us = copy.deepcopy(macro.DEFAULT_US_METRICS)
        cn = copy.deepcopy(macro.DEFAULT_CN_METRICS)
        cross = copy.deepcopy(macro.DEFAULT_CROSS_ASSET)
        commodities = macro.fetch_commodities_data(copy.deepcopy(macro.DEFAULT_COMMODITIES), dfii10=us["dfii10"]["value"])
        c_map = {c["id"]: c for c in commodities}
        self.assertEqual(c_map["copper"]["fundamentalGrade"], "STRONG_BULL")
        self.assertEqual(c_map["alumina"]["fundamentalGrade"], "BACK_WATCH")
        self.assertEqual(c_map["tungsten"]["fundamentalGrade"], "CACHE_WATCH")
        self.assertEqual(c_map["rebar"]["fundamentalGrade"], "CONTANGO_BEAR")
        self.assertEqual(c_map["gold"]["fundamentalGrade"], "PRECIOUS_HEDGE")

        engine_out = macro.run_hard_rule_engine(us, cn, cross_asset=cross, commodities=commodities)
        self.assertEqual(len(engine_out["hard_rules"]), 8)
        self.assertEqual(len(engine_out["barbell_allocation"]["breakdown"]), 5)
        total_w = sum(b["weight"] for b in engine_out["barbell_allocation"]["breakdown"])
        self.assertEqual(total_w, 100)

        pool = macro.enrich_equity_pool(copy.deepcopy(macro.DEFAULT_EQUITY_POOL), commodities, defense_weight=65)
        p_map = {s["code"]: s for s in pool}
        self.assertEqual(p_map["601899.SH"]["resonanceGrade"], "STRONG_RESONANCE")
        self.assertEqual(p_map["000807.SZ"]["resonanceGrade"], "COST_SQUEEZE_WATCH")
        self.assertEqual(p_map["920807.BJ"]["resonanceGrade"], "CACHE_WATCH")

    def test_shfe_warrant_table_parser_extracts_core_commodities(self):
        sample_html = """
        <table><tr><td>地区</td><td>仓库</td><td>期货</td><td>增减</td></tr><tr><td>铜</td><td>单位：吨</td></tr><tr><td>总计</td><td>6,053</td><td>-448</td></tr></table>
        <table><tr><td>地区</td><td>仓库</td><td>期货</td><td>增减</td></tr><tr><td>铝</td><td>单位：吨</td></tr><tr><td>总计</td><td>32470</td><td>-3294</td></tr></table>
        <table><tr><td>地区</td><td>仓库</td><td>期货</td><td>增减</td></tr><tr><td>氧化铝(仓库)</td><td>单位：吨</td></tr><tr><td>总计</td><td>192589</td><td>9593</td></tr></table>
        <table><tr><td>厂库</td><td>地区</td><td>期货</td><td>增减</td></tr><tr><td>氧化铝(厂库)</td><td>单位：吨</td></tr><tr><td>总计</td><td>9900</td><td>0</td></tr></table>
        <table><tr><td>地区</td><td>交割仓库</td><td>本日数量</td><td>增减</td></tr><tr><td>中质含硫原油</td><td>单位：桶</td></tr><tr><td>总计</td><td>3666000</td><td>0</td></tr></table>
        <table><tr><td>地区</td><td>仓库</td><td>期货</td><td>增减</td></tr><tr><td>铜(BC)</td><td>单位：吨</td></tr><tr><td>总计</td><td>427</td><td>0</td></tr></table>
        """
        parsed = macro._parse_shfe_warrant_tables(sample_html)
        self.assertEqual(parsed["copper"], {"qty": 6053, "chg": -448})
        self.assertEqual(parsed["aluminum"], {"qty": 32470, "chg": -3294})
        self.assertEqual(parsed["alumina"], {"qty": 202489, "chg": 9593})
        self.assertEqual(parsed["crude"], {"qty": 3666000, "chg": 0})

    def test_ashare_kline_falls_back_to_tencent_on_eastmoney_disconnect(self):
        fake_qq = {
            "quoteDate": "2026-11-20",
            "price": 21.01,
            "changePct": -1.59,
            "turnoverYi": 6.87,
            "trend_10d": [21.35, 21.01],
            "trend_dates": ["11-19", "11-20"]
        }
        with patch.object(macro.requests, "get", side_effect=ConnectionError("RemoteDisconnected")), \
             patch.object(macro.time, "sleep", return_value=None), \
             patch.object(macro, "_fetch_ashare_kline_tencent", return_value=fake_qq) as mock_qq:
            res = macro._fetch_ashare_completed_kline("000807.SZ", "2026-11-23", False)
        mock_qq.assert_called_once_with("000807.SZ", "2026-11-23", False)
        self.assertEqual(res["quoteDate"], "2026-11-20")
        self.assertEqual(res["price"], 21.01)


if __name__ == "__main__":
    unittest.main()
